var express = require('express');
var multer = require('multer');
var XLSX = require('xlsx');
var pdfParse = require('pdf-parse');
var fs = require('fs');
var path = require('path');

var app = express();
var PORT = process.env.PORT || 3000;
var DATA_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH || './data';
var DATA_FILE = path.join(DATA_DIR, 'manifest-data.json');
var upload = multer({ dest: path.join(DATA_DIR, 'uploads/') });

// Ensure data directories exist
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}
var PROFILES_DIR = path.join(DATA_DIR, 'profiles');
if (!fs.existsSync(PROFILES_DIR)) {
  fs.mkdirSync(PROFILES_DIR, { recursive: true });
}

// Initialize data
var defaultData = {
  generators: [],
  transporters: [],
  facilities: [],
  wasteStreams: [],
  manifests: [],
  profiles: []
};

function loadData() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      var raw = fs.readFileSync(DATA_FILE, 'utf8');
      return JSON.parse(raw);
    }
  } catch (e) {
    console.error('Error loading data:', e);
  }
  return JSON.parse(JSON.stringify(defaultData));
}

function saveData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
}

var data = loadData();
// Ensure profiles array exists for older data files
if (!data.profiles) { data.profiles = []; saveData(data); }

// Middleware
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true }));

// SSE connections
var sseClients = [];
app.get('/api/events', function(req, res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });
  res.write('data: connected\n\n');
  sseClients.push(res);
  req.on('close', function() {
    sseClients = sseClients.filter(function(c) { return c !== res; });
  });
});

function broadcast(event, payload) {
  var msg = 'event: ' + event + '\ndata: ' + JSON.stringify(payload) + '\n\n';
  sseClients.forEach(function(c) { c.write(msg); });
}

// Get all data
app.get('/api/data', function(req, res) {
  res.json(data);
});

// CRUD for each collection
var collections = ['generators', 'transporters', 'facilities', 'wasteStreams', 'manifests'];

collections.forEach(function(col) {
  // Get all
  app.get('/api/' + col, function(req, res) {
    res.json(data[col] || []);
  });

  // Create
  app.post('/api/' + col, function(req, res) {
    var item = req.body;
    item.id = Date.now().toString();
    item.createdAt = new Date().toISOString();
    if (!data[col]) data[col] = [];
    data[col].push(item);
    saveData(data);
    broadcast('update', { collection: col, action: 'create', item: item });
    res.json(item);
  });

  // Update
  app.put('/api/' + col + '/:id', function(req, res) {
    var idx = -1;
    for (var i = 0; i < (data[col] || []).length; i++) {
      if (data[col][i].id === req.params.id) { idx = i; break; }
    }
    if (idx === -1) return res.status(404).json({ error: 'Not found' });
    data[col][idx] = Object.assign({}, data[col][idx], req.body);
    saveData(data);
    broadcast('update', { collection: col, action: 'update', item: data[col][idx] });
    res.json(data[col][idx]);
  });

  // Delete single
  app.delete('/api/' + col + '/:id', function(req, res) {
    var before = (data[col] || []).length;
    data[col] = (data[col] || []).filter(function(item) { return item.id !== req.params.id; });
    if (data[col].length === before) return res.status(404).json({ error: 'Not found' });
    saveData(data);
    broadcast('update', { collection: col, action: 'delete', id: req.params.id });
    res.json({ success: true });
  });
});

// Waste Streams routes (HTML uses /api/waste-streams with hyphen)
app.get('/api/waste-streams', function(req, res) { res.json(data.wasteStreams || []); });
app.post('/api/waste-streams', function(req, res) {
  var item = req.body; item.id = Date.now().toString(); item.createdAt = new Date().toISOString();
  if (!data.wasteStreams) data.wasteStreams = [];
  data.wasteStreams.push(item); saveData(data);
  broadcast('update', { collection: 'wasteStreams', action: 'create', item: item }); res.json(item);
});
app.put('/api/waste-streams/:id', function(req, res) {
  var idx = -1;
  for (var i = 0; i < (data.wasteStreams || []).length; i++) { if (data.wasteStreams[i].id === req.params.id) { idx = i; break; } }
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  data.wasteStreams[idx] = Object.assign({}, data.wasteStreams[idx], req.body); saveData(data);
  broadcast('update', { collection: 'wasteStreams', action: 'update', item: data.wasteStreams[idx] }); res.json(data.wasteStreams[idx]);
});
app.delete('/api/waste-streams/:id', function(req, res) {
  var before = (data.wasteStreams || []).length;
  data.wasteStreams = (data.wasteStreams || []).filter(function(item) { return item.id !== req.params.id; });
  if (data.wasteStreams.length === before) return res.status(404).json({ error: 'Not found' });
  saveData(data); broadcast('update', { collection: 'wasteStreams', action: 'delete', id: req.params.id }); res.json({ success: true });
});

// ===== PROFILES (stored PDFs) =====
// List all stored profiles
app.get('/api/profiles', function(req, res) {
  res.json(data.profiles || []);
});

// Serve a profile PDF file
app.get('/api/profiles/:id/pdf', function(req, res) {
  var profile = null;
  for (var i = 0; i < (data.profiles || []).length; i++) {
    if (data.profiles[i].id === req.params.id) { profile = data.profiles[i]; break; }
  }
  if (!profile) return res.status(404).json({ error: 'Profile not found' });
  var filePath = path.join(PROFILES_DIR, profile.filename);
  if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'PDF file not found' });
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', 'inline; filename="' + profile.originalName + '"');
  fs.createReadStream(filePath).pipe(res);
});

// Delete a stored profile
app.delete('/api/profiles/:id', function(req, res) {
  var idx = -1;
  for (var i = 0; i < (data.profiles || []).length; i++) {
    if (data.profiles[i].id === req.params.id) { idx = i; break; }
  }
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  var profile = data.profiles[idx];
  // Delete the PDF file
  var filePath = path.join(PROFILES_DIR, profile.filename);
  try { fs.unlinkSync(filePath); } catch (e) {}
  data.profiles.splice(idx, 1);
  saveData(data);
  broadcast('update', { collection: 'profiles', action: 'delete', id: req.params.id });
  res.json({ success: true });
});

// Merge/move profiles from one generator to another
app.post('/api/profiles/merge', function(req, res) {
  var fromGen = req.body.from;
  var toGen = req.body.to;
  if (!fromGen || !toGen) return res.status(400).json({ error: 'Both "from" and "to" generator names required' });
  var count = 0;
  for (var i = 0; i < (data.profiles || []).length; i++) {
    if (data.profiles[i].generatorName === fromGen) {
      data.profiles[i].generatorName = toGen;
      count++;
    }
  }
  if (count === 0) return res.status(404).json({ error: 'No profiles found for generator "' + fromGen + '"' });
  saveData(data);
  broadcast('update', { collection: 'profiles', action: 'merge' });
  res.json({ success: true, moved: count });
});

// Helper: save an imported profile PDF
function saveProfilePDF(reqFile, profileId, source, wasteStreamName, generatorName, originalFilename) {
  var ext = path.extname(originalFilename || reqFile.originalname || '.pdf');
  var safeFilename = profileId + '-' + Date.now() + ext;
  var destPath = path.join(PROFILES_DIR, safeFilename);
  fs.copyFileSync(reqFile.path, destPath);
  var profileRecord = {
    id: Date.now().toString(),
    profileId: profileId,
    source: source,
    wasteStreamName: wasteStreamName,
    generatorName: generatorName,
    originalName: originalFilename || reqFile.originalname || 'profile.pdf',
    filename: safeFilename,
    fileSize: reqFile.size || 0,
    importedAt: new Date().toISOString()
  };
  if (!data.profiles) data.profiles = [];
  data.profiles.push(profileRecord);
  saveData(data);
  broadcast('update', { collection: 'profiles', action: 'create', item: profileRecord });
  return profileRecord;
}

// Delete ALL generators
app.delete('/api/generators', function(req, res) {
  var count = data.generators.length;
  data.generators = [];
  saveData(data);
  broadcast('update', { collection: 'generators', action: 'clearAll' });
  res.json({ success: true, deleted: count });
});

// QuickBooks Import
app.post('/api/import/quickbooks', upload.single('file'), function(req, res) {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    var workbook = XLSX.readFile(req.file.path);
    var sheetName = workbook.SheetNames[0];
    var sheet = workbook.Sheets[sheetName];
    var rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });

    // Find the header row (look for "Customer full name" or similar)
    var headerRowIdx = -1;
    var headers = [];
    for (var r = 0; r < Math.min(rows.length, 10); r++) {
      var row = rows[r];
      if (!row) continue;
      for (var c = 0; c < row.length; c++) {
        var cell = String(row[c] || '').toLowerCase().trim();
        if (cell.indexOf('customer') !== -1 && cell.indexOf('name') !== -1) {
          headerRowIdx = r;
          headers = row.map(function(h) { return String(h || '').trim(); });
          break;
        }
      }
      if (headerRowIdx !== -1) break;
    }

    if (headerRowIdx === -1) {
      return res.status(400).json({ error: 'Could not find header row with Customer name column' });
    }

    // Flexible column finder
    function findCol(keywords) {
      for (var i = 0; i < headers.length; i++) {
        var h = headers[i].toLowerCase();
        var match = true;
        for (var k = 0; k < keywords.length; k++) {
          if (h.indexOf(keywords[k].toLowerCase()) === -1) { match = false; break; }
        }
        if (match) return i;
      }
      return -1;
    }

    var nameCol = findCol(['customer', 'name']);
    var phoneCol = findCol(['phone']);
    var billCol = findCol(['bill']);
    var shipCol = findCol(['ship']);
    var epaCol = findCol(['epa']);

    // Parse address string like "4780 E Carmen Ave Fresno CA 93702"
    function parseAddress(addr) {
      if (!addr) return { street: '', city: '', state: '', zip: '' };
      var str = String(addr).trim();
      var parts = str.split(',');
      if (parts.length >= 2) {
        var street = parts[0].trim();
        var rest = parts.slice(1).join(',').trim();
        var stateZipMatch = rest.match(/\s*([A-Za-z]{2})\s+(\d{5}(-\d{4})?)\s*$/);
        if (stateZipMatch) {
          var city = rest.substring(0, rest.length - stateZipMatch[0].length).trim().replace(/,\s*$/, '');
          return { street: street, city: city, state: stateZipMatch[1].toUpperCase(), zip: stateZipMatch[2] };
        }
        var fallback = rest.match(/^(.+?)\s+([A-Za-z]{2})\s+(\d{5}(-\d{4})?)/);
        if (fallback) {
          return { street: street, city: fallback[1].trim(), state: fallback[2].toUpperCase(), zip: fallback[3] };
        }
        return { street: street, city: rest, state: '', zip: '' };
      }
      var noComma = str.match(/^(.+?)\s+([A-Za-z]+)\s+([A-Za-z]{2})\s+(\d{5}(-\d{4})?)$/);
      if (noComma) {
        return { street: noComma[1], city: noComma[2], state: noComma[3].toUpperCase(), zip: noComma[4] };
      }
      return { street: str, city: '', state: '', zip: '' };
    }

    var imported = 0;
    for (var r2 = headerRowIdx + 1; r2 < rows.length; r2++) {
      var dataRow = rows[r2];
      if (!dataRow || !dataRow[nameCol]) continue;
      var name = String(dataRow[nameCol] || '').trim();
      if (!name) continue;

      var exists = false;
      for (var d = 0; d < data.generators.length; d++) {
        if (data.generators[d].name === name) { exists = true; break; }
      }
      if (exists) continue;

      var phone = phoneCol !== -1 ? String(dataRow[phoneCol] || '').trim() : '';
      var billAddr = billCol !== -1 ? parseAddress(dataRow[billCol]) : { street: '', city: '', state: '', zip: '' };
      var shipAddr = shipCol !== -1 ? parseAddress(dataRow[shipCol]) : { street: '', city: '', state: '', zip: '' };
      var epaId = epaCol !== -1 ? String(dataRow[epaCol] || '').trim() : '';

      data.generators.push({
        id: Date.now().toString() + '_' + r2,
        name: name,
        epaId: epaId,
        phone: phone,
        contactName: '',
        emergencyPhone: '',
        mailAddress: billAddr.street,
        mailCity: billAddr.city,
        mailState: billAddr.state,
        mailZip: billAddr.zip,
        siteAddress: shipAddr.street,
        city: shipAddr.city,
        state: shipAddr.state,
        zip: shipAddr.zip,
        createdAt: new Date().toISOString()
      });
      imported++;
    }

    saveData(data);
    broadcast('update', { collection: 'generators', action: 'import' });
    try { fs.unlinkSync(req.file.path); } catch (e) {}

    res.json({
      success: true,
      imported: imported,
      total: data.generators.length,
      columns: { name: nameCol, phone: phoneCol, bill: billCol, ship: shipCol, epa: epaCol },
      headers: headers
    });
  } catch (err) {
    console.error('Import error:', err);
    res.status(500).json({ error: err.message });
  }
});

// Waste Profile PDF Import
app.post('/api/import/waste-profile', upload.single('file'), function(req, res) {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    var buffer = fs.readFileSync(req.file.path);
    pdfParse(buffer).then(function(pdfData) {
      var text = pdfData.text;
      var lines = text.split('\n').map(function(l) { return l.trim(); });
      var fullText = text;

      // Helper: find text after a label
      function findAfter(label) {
        var idx = fullText.indexOf(label);
        if (idx === -1) return '';
        var after = fullText.substring(idx + label.length);
        // Get text until next newline or next label-like pattern
        var match = after.match(/^\s*:?\s*([^\n]*)/);
        return match ? match[1].trim() : '';
      }

      // Helper: find text between two labels
      function findBetween(startLabel, endLabel) {
        var startIdx = fullText.indexOf(startLabel);
        if (startIdx === -1) return '';
        var after = fullText.substring(startIdx + startLabel.length);
        var endIdx = endLabel ? after.indexOf(endLabel) : after.indexOf('\n');
        if (endIdx === -1) endIdx = Math.min(after.length, 200);
        return after.substring(0, endIdx).trim().replace(/^\s*:?\s*/, '');
      }

      // ===== DETECT EWS PROFILE FORMAT =====
      var isEWS = fullText.indexOf('Environmental Waste Solution') !== -1 || fullText.indexOf('EWS') !== -1 && fullText.indexOf('GENERATOR WASTE PROFILE SHEET') !== -1;

      if (isEWS) {
        console.log('Detected EWS profile format');

        // Profile Number - just need "EWS" + the numbers after it (e.g. EWS41291)
        var ewsProfileId = '';
        // First try: collapse all spaces/underscores from the full text and look for EWS followed by digits
        var collapsed = fullText.replace(/[\s_]+/g, '');
        var ewsNumMatch = collapsed.match(/EWS(\d{3,})/i);
        if (ewsNumMatch) {
          ewsProfileId = 'EWS' + ewsNumMatch[1];
        }
        // Fallback: check near Profile Number label
        if (!ewsProfileId) {
          var pidLine = fullText.match(/Profile Number[:\s]+([^\n]+)/i);
          if (pidLine) {
            var pidCollapsed = pidLine[1].replace(/[\s_]+/g, '');
            var pidNumMatch = pidCollapsed.match(/EWS(\d{3,})/i);
            if (pidNumMatch) ewsProfileId = 'EWS' + pidNumMatch[1];
          }
        }

        // Generator info
        var ewsGenName = '';
        var gnMatch = fullText.match(/Generator Name[:\s]+([^\n]+)/i);
        if (gnMatch) ewsGenName = gnMatch[1].trim();

        var ewsEpaId = '';
        // EPA ID may be on same line or a few lines below the label
        var epaMatch = fullText.match(/EPA Identification Number[:\s]+([A-Z]{2}[A-Z0-9]{8,})/i);
        if (epaMatch) {
          ewsEpaId = epaMatch[1].trim();
        } else {
          // Look for standalone EPA ID pattern (2 letters + 9+ alphanumeric) near the label
          var epaBlock = fullText.match(/EPA Identification Number[\s\S]{0,200}/i);
          if (epaBlock) {
            var epaLineMatch = epaBlock[0].match(/\b([A-Z]{2}[A-Z0-9]{8,12})\b/);
            if (epaLineMatch) ewsEpaId = epaLineMatch[1].trim();
          }
        }

        // Is EPA Hazardous Waste?
        var ewsIsHaz = false;
        var hazMatch = fullText.match(/US EPA HAZARDOUS WASTE[^)]*\)?\s*\??\s*(YES|NO)/i);
        if (hazMatch) ewsIsHaz = hazMatch[1].toUpperCase() === 'YES';

        // State Codes
        var ewsStateCodes = '';
        var scMatch = fullText.match(/State Codes[:\s]+([^\n]+)/i);
        if (scMatch) {
          ewsStateCodes = scMatch[1].trim().replace(/[A-Za-z]+-?/g, '').replace(/\s+/g, ' ').trim();
          if (ewsStateCodes.toLowerCase() === 'none' || ewsStateCodes === '') ewsStateCodes = '';
        }

        // Waste Name (field 11a or "Common Waste Name" or "Waste Name")
        var ewsWasteName = '';
        var wnMatch = fullText.match(/(?:Common )?Waste Name[:\s]+([^\n]+)/i);
        if (wnMatch) ewsWasteName = wnMatch[1].trim();
        // Clean trailing field numbers
        ewsWasteName = ewsWasteName.replace(/\s*\d+\.\s*US DOT.*$/i, '').trim();

        // DOT Proper Shipping Name (field 11b or 13)
        var ewsDotDesc = '';
        var dotMatch = fullText.match(/(?:US )?DOT Proper Shipping Name[:\s]+([^\n]+)/i);
        if (dotMatch) ewsDotDesc = dotMatch[1].trim();
        // Clean up trailing field labels
        ewsDotDesc = ewsDotDesc.replace(/\s*\d+\.\s*Physical.*$/i, '').trim();

        // Try to extract UN/NA from the DOT description itself
        var ewsUnNum = '';
        var ewsUnMatch = ewsDotDesc.match(/\b(UN\d{4,5}|NA\d{4,5})\b/i);
        if (ewsUnMatch) ewsUnNum = ewsUnMatch[1];

        // Hazard class and packing group from description if present
        var ewsHazClass = '';
        var ewsPG = '';
        var ewsHcMatch = ewsDotDesc.match(/\b(\d\.\d)\b/);
        if (ewsHcMatch && ewsIsHaz) ewsHazClass = ewsHcMatch[1];
        var ewsPgMatch = ewsDotDesc.match(/\bPG\s*(I{1,3})\b/i) || ewsDotDesc.match(/\b(I{1,3})\s*$/);
        if (ewsPgMatch && ewsIsHaz) ewsPG = ewsPgMatch[1];

        // Uppercase DOT description
        if (ewsDotDesc) ewsDotDesc = ewsDotDesc.toUpperCase();
        // If N.O.S., append common name
        if (ewsDotDesc && ewsDotDesc.match(/N\.O\.S\.?\s*$/) && ewsWasteName) {
          ewsDotDesc = ewsDotDesc + ' (' + ewsWasteName.toUpperCase() + ')';
        }

        // RCRA waste codes from description or text
        var ewsRcraCodes = '';
        var ewsRcraMatch = fullText.match(/RCRA Waste Codes?[:\s]+([A-Z0-9\s,]+)/i);
        if (ewsRcraMatch) {
          ewsRcraCodes = ewsRcraMatch[1].trim().replace(/\s+/g, ' ');
          if (ewsRcraCodes.toLowerCase() === 'none') ewsRcraCodes = '';
        }

        // Physical state
        var ewsPhysical = '';
        var physMatch = fullText.match(/Physical State[^X\n]*X\s*(\w+)/i);
        if (physMatch) ewsPhysical = physMatch[1].trim();

        // Composition
        var ewsComposition = '';
        var compMatch = fullText.match(/Waste Composition[:\s]+([^\n]+([\n][A-Z][\w\s%.-]+)*)/i);
        if (compMatch) ewsComposition = compMatch[1].replace(/\n/g, '; ').trim();

        // pH and Flash Point from characteristic section
        var ewsPH = '';
        var phMatch = fullText.match(/pH[:\s]+([\d.-]+)/i);
        if (phMatch) ewsPH = phMatch[1].trim();

        var ewsFlash = '';
        var fpMatch = fullText.match(/Flash Point[:\s]+([^\n]+)/i);
        if (fpMatch) ewsFlash = fpMatch[1].trim().split(/\s{2,}/)[0].trim();

        var ewsWasteStream = {
          id: Date.now().toString(),
          name: ewsWasteName || 'EWS Profile ' + ewsProfileId,
          dotDescription: ewsDotDesc,
          hm: ewsIsHaz ? 'X' : '',
          containerType: '',
          unit: '',
          wasteCodes: ewsRcraCodes,
          unNum: ewsUnNum,
          hazardClass: ewsHazClass,
          packingGroup: ewsPG,
          stateWasteCodes: ewsStateCodes,
          ergNum: '',
          profileId: ewsProfileId,
          source: 'EWS',
          generatorName: ewsGenName,
          generatorEpaId: ewsEpaId,
          composition: ewsComposition,
          physicalState: ewsPhysical,
          pH: ewsPH,
          flashPoint: ewsFlash,
          createdAt: new Date().toISOString()
        };

        // Check for duplicate
        var ewsExists = false;
        for (var ed = 0; ed < (data.wasteStreams || []).length; ed++) {
          if (data.wasteStreams[ed].name === ewsWasteStream.name) { ewsExists = true; break; }
        }
        if (ewsExists) {
          try { fs.unlinkSync(req.file.path); } catch (e) {}
          return res.json({ success: false, error: 'A waste stream named "' + ewsWasteStream.name + '" already exists.', extracted: ewsWasteStream });
        }

        if (!data.wasteStreams) data.wasteStreams = [];
        data.wasteStreams.push(ewsWasteStream);
        saveData(data);
        broadcast('update', { collection: 'wasteStreams', action: 'create', item: ewsWasteStream });
        saveProfilePDF(req.file, ewsProfileId, 'EWS', ewsWasteStream.name, ewsGenName, req.file.originalname);
        try { fs.unlinkSync(req.file.path); } catch (e) {}

        return res.json({ success: true, wasteStream: ewsWasteStream, source: 'EWS' });
      }

      // ===== DETECT SAMEX PROFILE FORMAT =====
      var isSamex = fullText.indexOf('WASTE PROFILE') !== -1 && fullText.indexOf('A.- Generator Information') !== -1 && fullText.indexOf('I.- Trans Information') !== -1;

      if (isSamex) {
        console.log('Detected Samex profile format');

        // Profile Number - "Profile# 36183" -> "SMX36183"
        var smxProfileId = '';
        var smxPidMatch = fullText.match(/Profile#\s*(\d+)/i);
        if (smxPidMatch) smxProfileId = 'SMX' + smxPidMatch[1];

        // Generator Name - appears between "Generator Module" and "Name:" in raw text
        var smxGenName = '';
        var genBlock = fullText.match(/Generator (?:Module|Information)[\s\S]{0,500}/i);
        if (genBlock) {
          // Try the line right after "Generator Module" text and before "Name:"
          var genLines = genBlock[0].split('\n');
          for (var gl = 0; gl < genLines.length; gl++) {
            var gline = genLines[gl].trim();
            if (gline.length > 10 && gline === gline.toUpperCase() && gline.indexOf('*') === -1 && gline.indexOf('Generator') === -1 && gline.indexOf('WASTE') === -1 && gline.indexOf('ALL ') === -1 && gline.indexOf('Module') === -1) {
              smxGenName = gline;
              break;
            }
          }
        }
        // Fallback: try "Name:" label directly
        if (!smxGenName) {
          var nameMatch = fullText.match(/(?:^|\n)\s*Name:\s*([^\n]+)/);
          if (nameMatch && nameMatch[1].trim().length > 3) smxGenName = nameMatch[1].trim();
        }

        // EPA ID
        var smxEpaId = '';
        var smxEpaMatch = fullText.match(/EPA ID#?[:\s]+([A-Z]{2}[A-Z0-9]{8,})/i);
        if (!smxEpaMatch) {
          // EPA ID may be on a separate line
          var epaBlock = fullText.match(/EPA ID#?[\s\S]{0,100}/i);
          if (epaBlock) {
            var epaLineMatch = epaBlock[0].match(/\b([A-Z]{2}[A-Z0-9]{8,12})\b/);
            if (epaLineMatch) smxEpaId = epaLineMatch[1];
          }
        } else {
          smxEpaId = smxEpaMatch[1];
        }

        // Generator address
        var smxAddress = '';
        var smxCity = '';
        var smxState = '';
        var smxZip = '';
        var siteAddrMatch = fullText.match(/Site\s*Address[:\s]+([^\n]+)/i);
        if (siteAddrMatch) smxAddress = siteAddrMatch[1].trim();
        // City/State/Zip after Site Address section
        var addrBlock = fullText.match(/Site\s*Address[\s\S]{0,300}/i);
        if (addrBlock) {
          var cityMatch = addrBlock[0].match(/City[:\s]+([A-Z][A-Z\s]+)/i);
          if (cityMatch) smxCity = cityMatch[1].trim();
          var stateMatch = addrBlock[0].match(/State[:\s]+([A-Z]{2})/i);
          if (stateMatch) smxState = stateMatch[1].trim();
          var zipMatch = addrBlock[0].match(/Zip[:\s]+(\d{5})/);
          if (zipMatch) smxZip = zipMatch[1].trim();
        }

        // State Waste Codes - in Samex PDFs, the value may be on a different line
        var smxStateCodes = '';
        var smxScBlock = fullText.match(/State Waste\s*Code\(s\)[\s\S]{0,300}?(?=Waste Common|Generating)/i);
        if (smxScBlock) {
          // Find standalone numbers (like 331) that aren't part of other fields
          var scNums = smxScBlock[0].match(/\b(\d{3,4})\b/g);
          if (scNums) {
            // Filter out numbers that are likely D-code numbers
            var stateOnly = [];
            for (var sn = 0; sn < scNums.length; sn++) {
              if (!smxScBlock[0].match(new RegExp('[DFKPU]' + scNums[sn]))) stateOnly.push(scNums[sn]);
            }
            smxStateCodes = stateOnly.join(' ');
          }
        }

        // EPA Waste Codes
        var smxWasteCodes = '';
        var smxEpaWcBlock = fullText.match(/EPA Waste\s*Code\(s\)[\s\S]{0,200}/i);
        if (smxEpaWcBlock) {
          // Look for D/F/K/P/U codes in the block
          var wcMatches = smxEpaWcBlock[0].match(/\b[DFKPU]\d{3}\b/g);
          if (wcMatches) smxWasteCodes = wcMatches.join(' ');
        }

        // Is hazardous? Based on whether we have EPA waste codes or DOT Hazardous: Yes
        var smxIsHaz = smxWasteCodes.length > 0;
        var dotHazMatch = fullText.match(/DOT Hazardous[:\s]+(Yes|No)/i);
        if (dotHazMatch && dotHazMatch[1].toUpperCase() === 'YES') smxIsHaz = true;

        // Waste Common Name
        var smxWasteName = '';
        var smxWnMatch = fullText.match(/Waste Common\s*Name[:\s]+([^\n]+)/i);
        if (smxWnMatch) smxWasteName = smxWnMatch[1].trim();
        // Sometimes name is on the next line
        if (!smxWasteName) {
          var wnBlock = fullText.match(/Waste Common\s*\n([^\n]+)/i);
          if (wnBlock) smxWasteName = wnBlock[1].replace(/Name[:\s]*/i, '').trim();
        }

        // DOT Shipping Name
        var smxDotDesc = '';
        var smxDotMatch = fullText.match(/DOT Shipping Name[:\s]+([^\n]+)/i);
        if (smxDotMatch) smxDotDesc = smxDotMatch[1].trim();

        // UN/NA# - explicit field
        var smxUnNum = '';
        var smxUnMatch = fullText.match(/UN\/NA#[:\s]+((?:UN|NA)\d{4,5})/i);
        if (smxUnMatch) smxUnNum = smxUnMatch[1];
        // Fallback: extract from DOT shipping name
        if (!smxUnNum) {
          var dotUnMatch = (smxDotDesc || '').match(/\b(UN\d{4,5}|NA\d{4,5})\b/i);
          if (dotUnMatch) smxUnNum = dotUnMatch[1];
        }

        // Hazard Class - explicit field
        var smxHazClass = '';
        var smxHcMatch = fullText.match(/Hazard Class[:\s]+(\d(?:\.\d)?)\b/i);
        if (smxHcMatch && smxIsHaz) smxHazClass = smxHcMatch[1];

        // ERG Number
        var smxErg = '';
        var smxErgMatch = fullText.match(/ERG#[:\s]+(\d{2,4})/i);
        if (smxErgMatch) smxErg = smxErgMatch[1];

        // Packing Group - "Group: II" or "Packaging Group: II"
        var smxPG = '';
        var smxPgMatch = fullText.match(/(?:Packaging\s*)?Group[:\s]+(I{1,3})\b/i);
        if (smxPgMatch && smxIsHaz) smxPG = smxPgMatch[1];

        // Physical State
        var smxPhysical = '';
        var physBlock = fullText.match(/Physical State[\s\S]{0,200}/i);
        if (physBlock) {
          // Look for the selected radio option (comes after the bullet markers in PDF text)
          if (physBlock[0].match(/Liquid/i)) smxPhysical = 'Liquid';
          else if (physBlock[0].match(/Solid/i)) smxPhysical = 'Solid';
          else if (physBlock[0].match(/Sludge/i)) smxPhysical = 'Sludge';
          else if (physBlock[0].match(/Gas/i)) smxPhysical = 'Gas';
        }

        // Container Size/Type
        var smxContainer = '';
        var smxContMatch = fullText.match(/Container \(Size\/Type\)[:\s]+([^\n]+)/i);
        if (smxContMatch) smxContainer = smxContMatch[1].trim();

        // Flash Point range
        var smxFlash = '';
        var flashBlock = fullText.match(/Flash Point[\s\S]{0,200}/i);
        if (flashBlock) {
          if (flashBlock[0].indexOf('<73') !== -1 && flashBlock[0].match(/\<73/)) smxFlash = '<73F';
          else if (flashBlock[0].match(/73-100/)) smxFlash = '73-100F';
          else if (flashBlock[0].match(/101-141/)) smxFlash = '101-141F';
          else if (flashBlock[0].match(/141-200/)) smxFlash = '141-200F';
          else if (flashBlock[0].match(/>200/)) smxFlash = '>200F';
        }

        // Chemical Composition - two-column table, values separated by blank lines
        // Pattern: Name, (blank), Min%, (blank), Max%, (blank), [2nd col name]
        var smxComposition = '';
        var compBlock = fullText.match(/H\.\-?\s*Chemical Composition[\s\S]{0,1500}?(?=I\.\-?\s*Trans)/i);
        if (compBlock) {
          var chemLines = [];
          // Strip blank lines to get clean sequence
          var compNonBlank = compBlock[0].split('\n').map(function(l){return l.trim()}).filter(function(l){return l.length > 0});
          // Find where actual data starts (after "Names)" header)
          var dataStart = -1;
          for (var cn = 0; cn < compNonBlank.length; cn++) {
            if (compNonBlank[cn] === 'Names)' && cn > compNonBlank.length / 2) { dataStart = cn + 1; break; }
          }
          if (dataStart > 0) {
            var ci = dataStart;
            while (ci < compNonBlank.length) {
              var chemName = compNonBlank[ci];
              // Check if this is a chemical name followed by min and max numbers
              if (chemName.match(/^[A-Za-z]/) && ci + 2 < compNonBlank.length) {
                var minVal = compNonBlank[ci + 1];
                var maxVal = compNonBlank[ci + 2];
                if (minVal.match(/^\d{1,3}$/) && maxVal.match(/^\d{1,3}$/)) {
                  chemLines.push(chemName + ' ' + minVal + '-' + maxVal + '%');
                  ci += 3;
                  // Skip 2nd column name if present (no numbers after it)
                  if (ci < compNonBlank.length && compNonBlank[ci].match(/^[A-Za-z]/) && (ci + 1 >= compNonBlank.length || !compNonBlank[ci + 1].match(/^\d{1,3}$/))) {
                    ci++;
                  }
                  continue;
                }
              }
              ci++;
            }
          }
          smxComposition = chemLines.join('; ');
        }

        // Build the DOT description for the manifest
        // Strip UN/NA number, hazard class, and PG from the shipping name since they're stored separately
        var smxDotForManifest = smxDotDesc.toUpperCase();
        // Remove UN/NA number from front (loadWasteStream will re-add from unNum field)
        smxDotForManifest = smxDotForManifest.replace(/\b(UN|NA)\d{4,5}\s*,?\s*/gi, '').trim();
        // Remove trailing ", 3, II" or ", 3, III" etc (hazard class + PG at end)
        if (smxHazClass) smxDotForManifest = smxDotForManifest.replace(new RegExp(',\\s*' + smxHazClass.replace('.', '\\.') + '\\s*$'), '').replace(new RegExp(',\\s*' + smxHazClass.replace('.', '\\.') + '\\s*,'), ',');
        if (smxPG) smxDotForManifest = smxDotForManifest.replace(new RegExp(',\\s*' + smxPG + '\\s*$', 'i'), '');
        // Clean up trailing/leading commas and spaces
        smxDotForManifest = smxDotForManifest.replace(/^[\s,]+|[\s,]+$/g, '').replace(/,\s*,/g, ',').trim();

        var smxWasteStream = {
          id: Date.now().toString(),
          name: smxWasteName || 'Samex Profile ' + smxProfileId,
          dotDescription: smxDotForManifest,
          hm: smxIsHaz ? 'X' : '',
          containerType: smxContainer,
          unit: '',
          wasteCodes: smxWasteCodes,
          unNum: smxUnNum,
          hazardClass: smxHazClass,
          packingGroup: smxPG,
          stateWasteCodes: smxStateCodes,
          ergNum: smxErg,
          profileId: smxProfileId,
          source: 'Samex',
          generatorName: smxGenName,
          generatorEpaId: smxEpaId,
          generatorAddress: smxAddress,
          generatorCity: smxCity,
          generatorState: smxState,
          generatorZip: smxZip,
          composition: smxComposition,
          physicalState: smxPhysical,
          flashPoint: smxFlash,
          createdAt: new Date().toISOString()
        };

        console.log('Samex parsed:', JSON.stringify(smxWasteStream, null, 2));

        // Check for duplicate
        var smxExists = false;
        for (var sd = 0; sd < (data.wasteStreams || []).length; sd++) {
          if (data.wasteStreams[sd].profileId === smxProfileId) { smxExists = true; break; }
        }
        if (smxExists) {
          try { fs.unlinkSync(req.file.path); } catch (e) {}
          return res.json({ success: false, error: 'A waste stream with profile "' + smxProfileId + '" already exists.', extracted: smxWasteStream });
        }

        if (!data.wasteStreams) data.wasteStreams = [];
        data.wasteStreams.push(smxWasteStream);
        saveData(data);
        broadcast('update', { collection: 'wasteStreams', action: 'create', item: smxWasteStream });
        saveProfilePDF(req.file, smxProfileId, 'Samex', smxWasteStream.name, smxGenName, req.file.originalname);
        try { fs.unlinkSync(req.file.path); } catch (e) {}

        return res.json({ success: true, wasteStream: smxWasteStream, source: 'Samex' });
      }

      // ===== REPUBLIC PROFILE FORMAT (original parser) =====
      // Extract fields
      var commonName = '';
      var dotDescription = '';
      var isHazMat = false;
      //var containerType = ''; // excluded - varies per shipment
      //var unitOfMeasure = ''; // excluded - varies per shipment
      var wasteCodes = '';
      var unNum = '';
      var hazardClass = '';
      var packingGroup = '';
      var stateWasteCodes = '';
      var ergNum = '';

      // B.1 Common Name
      for (var i = 0; i < lines.length; i++) {
        if (lines[i].indexOf('Common Name') !== -1 && lines[i].indexOf(':') !== -1) {
          commonName = lines[i].split(':').slice(1).join(':').trim();
          if (!commonName && i + 1 < lines.length) commonName = lines[i + 1].trim();
          break;
        }
      }
      // Also try after the label in full text
      if (!commonName) {
        var cnMatch = fullText.match(/Common Name[:\s]+([A-Z][A-Z\s,.-]+)/i);
        if (cnMatch) commonName = cnMatch[1].trim();
      }

      // C.1 Proper Shipping Name (DOT description)
      var psnMatch = fullText.match(/Proper Shipping Name[:\s]+([^\n]+)/i);
      if (psnMatch) dotDescription = psnMatch[1].trim();
      // Clean up - remove trailing field labels
      dotDescription = dotDescription.replace(/\s*(2\.\s*Additional|RQ Threshold|UN\/NA).*$/i, '').trim();

      // DOT Hazardous Materials
      var dotHazMatch = fullText.match(/DOT Hazardous Materials\?\s*(.*?)Proper/i);
      if (dotHazMatch) {
        isHazMat = dotHazMatch[1].indexOf('Yes') !== -1;
      }

      // UN/NA# - try multiple patterns since pdf-parse layouts vary
      var unMatch = fullText.match(/UN\/NA\s*#\s*:?\s*(UN\d{3,5}|NA\d{3,5})/i);
      if (!unMatch) unMatch = fullText.match(/UN\/NA\s*(?:Number|#|No\.?)\s*:?\s*(UN\d{3,5}|NA\d{3,5})/i);
      if (!unMatch) unMatch = fullText.match(/(UN\d{4,5})/);
      if (!unMatch) unMatch = fullText.match(/(NA\d{4,5})/);
      if (unMatch && isHazMat) unNum = unMatch[1].trim();

      // Hazard Class - only match valid DOT hazard classes (1-9, with optional subdivision like 3, 4.1, 6.1, 8)
      var hcMatch = fullText.match(/Hazard Class[:\s]+(\d(?:\.\d)?)\b/i);
      if (hcMatch && isHazMat) hazardClass = hcMatch[1].trim();

      // Packing Group - only match valid values (I, II, III or PG I, PG II, PG III)
      var pgMatch = fullText.match(/Packing Group[:\s]+(I{1,3}|PG\s*I{1,3})\b/i);
      if (pgMatch && isHazMat) packingGroup = pgMatch[1].trim();

      // ERG#
      var ergMatch = fullText.match(/ERG#[:\s]+(\S+)/i);
      if (ergMatch) ergNum = ergMatch[1].trim();

      // Container Type and Unit of Measure excluded - vary per shipment

      // RCRA Waste Codes (E.3) - strip "None" so it comes through empty
      var rcraMatch = fullText.match(/RCRA Waste Codes[:\s]+([A-Z0-9\s,]+)/i);
      if (rcraMatch) {
        wasteCodes = rcraMatch[1].trim().replace(/\s+/g, ' ').replace(/If None.*$/i, '').trim();
        if (wasteCodes.toLowerCase() === 'none') wasteCodes = '';
      }

      // State Waste Codes (E.2) - grab just the code, strip letter prefixes (CA-122 -> 122)
      var stMatch = fullText.match(/State Waste Codes[:\s]+([A-Z0-9][A-Z0-9\s,.-]*)/i);
      if (stMatch) {
        var rawStateCodes = stMatch[1].trim().split('\n')[0].trim();
        // Strip letter prefixes like CA-, AZ- etc. Keep only numbers
        stateWasteCodes = rawStateCodes.replace(/[A-Za-z]+-?/g, '').replace(/\s+/g, ' ').trim();
      }

      // Uppercase entire DOT description to match proper DOT style
      if (dotDescription) {
        dotDescription = dotDescription.toUpperCase();
      }
      // If DOT description contains N.O.S., append common name in parentheses
      if (dotDescription && dotDescription.match(/N\.O\.S\.?\s*$/i) && commonName) {
        dotDescription = dotDescription + ' (' + commonName.toUpperCase() + ')';
      }

      // Build the waste stream template
      var profileId = '';
      var pidMatch = fullText.match(/Profile\s*ID[:\s]+(\d+)/i);
      if (pidMatch) profileId = pidMatch[1];

      // Extract generator name from Section A / A.1
      var repGenName = '';
      var repGenMatch = fullText.match(/A\.?\s*1\.?\s*Generator[:\s]+([^\n]+)/i) ||
                        fullText.match(/Generator Name[:\s]+([^\n]+)/i) ||
                        fullText.match(/Section A[\s\S]{0,300}Generator[:\s]+([^\n]+)/i);
      if (repGenMatch) repGenName = repGenMatch[1].trim();
      // If not found, try looking for a line after "A.1" or "Generator" label
      if (!repGenName) {
        var genBlock = fullText.match(/Generator[\s\S]{0,200}/i);
        if (genBlock) {
          var gbLines = genBlock[0].split('\n');
          for (var gb = 1; gb < gbLines.length; gb++) {
            var gbl = gbLines[gb].trim();
            if (gbl.length > 5 && !gbl.match(/^(address|city|state|zip|phone|contact|fax|email|site|mailing)/i)) {
              repGenName = gbl;
              break;
            }
          }
        }
      }

      // Detect generic "Add Gen" profiles - Various Sites means no specific generator
      var isAddGen = repGenName.match(/various\s*sites/i) || repGenName.match(/multiple\s*generators/i) || repGenName.match(/add\s*gen/i);
      var profileGenName = isAddGen ? 'Republic / USE Add Gens' : repGenName;

      var wasteStream = {
        id: Date.now().toString(),
        name: commonName || 'Imported Profile ' + profileId,
        dotDescription: dotDescription,
        hm: isHazMat ? 'X' : '',
        containerType: '',
        unit: '',
        wasteCodes: wasteCodes,
        unNum: unNum,
        hazardClass: hazardClass,
        packingGroup: packingGroup,
        stateWasteCodes: stateWasteCodes,
        ergNum: ergNum,
        profileId: profileId,
        createdAt: new Date().toISOString()
      };

      // Check for duplicate by name
      var exists = false;
      for (var d = 0; d < (data.wasteStreams || []).length; d++) {
        if (data.wasteStreams[d].name === wasteStream.name) { exists = true; break; }
      }

      if (exists) {
        try { fs.unlinkSync(req.file.path); } catch (e) {}
        return res.json({
          success: false,
          error: 'A waste stream named "' + wasteStream.name + '" already exists.',
          extracted: wasteStream
        });
      }

      if (!data.wasteStreams) data.wasteStreams = [];
      data.wasteStreams.push(wasteStream);
      saveData(data);
      broadcast('update', { collection: 'wasteStreams', action: 'create', item: wasteStream });
      saveProfilePDF(req.file, profileId || wasteStream.id, 'Republic', wasteStream.name, profileGenName, req.file.originalname);
      try { fs.unlinkSync(req.file.path); } catch (e) {}

      res.json({
        success: true,
        wasteStream: wasteStream
      });
    }).catch(function(err) {
      console.error('PDF parse error:', err);
      try { fs.unlinkSync(req.file.path); } catch (e) {}
      res.status(500).json({ error: 'Failed to parse PDF: ' + err.message });
    });
  } catch (err) {
    console.error('Waste profile import error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// EPA Form 8700-22 Print Map
// 10 CPI (chars per inch), 6 LPI (lines per inch), 66 lines/page
// Box 5: Mailing address LEFT side, Site address RIGHT side
// ============================================================
var FORM_8700_MAP = {
  // 12 CPI with tractor pin all the way left
  // Box 1 - Generator's US EPA ID Number
  generatorEpaId:     { row: 4, col: 18 },
  // Box 2 - Page __ of __
  page:               { row: 4, col: 40 },
  totalPages:         { row: 4, col: 43 },
  // Box 3 - Emergency Response Phone
  emergencyPhone:     { row: 4, col: 47 },
  // Box 5 - Generator
  generatorName:      { row: 6, col: 16 },
  // Mailing Address (LEFT side of Box 5)
  generatorMailAddr:  { row: 7, col: 16 },
  generatorMailCity:  { row: 8, col: 16 },
  generatorPhone:     { row: 8, col: 34 },
  // Site Address (RIGHT side of Box 5)
  generatorSiteAddr:  { row: 7, col: 48 },
  generatorSiteCity:  { row: 8, col: 48 },
  // Box 6 - Transporter 1
  transporter1Name:   { row: 10, col: 8 },
  transporter1EpaId:  { row: 10, col: 62 },
  // Box 7 - Transporter 2
  transporter2Name:   { row: 12, col: 8 },
  transporter2EpaId:  { row: 12, col: 62 },
  // Box 8 - Designated Facility
  facilityName:       { row: 14, col: 8 },
  facilityEpaId:      { row: 14, col: 62 },
  facilityAddress:    { row: 15, col: 8 },
  facilityPhone:      { row: 16, col: 8 },
  facilityCity:       { row: 16, col: 24 },
  facilityState:      { row: 16, col: 38 },
  facilityZip:        { row: 16, col: 42 },
  // Box 9a - HM
  waste1hm:           { row: 21, col: 4 },
  waste2hm:           { row: 24, col: 4 },
  waste3hm:           { row: 27, col: 4 },
  waste4hm:           { row: 30, col: 4 },
  // Box 9b - Description
  waste1desc:         { row: 21, col: 9 },
  waste2desc:         { row: 24, col: 9 },
  waste3desc:         { row: 27, col: 9 },
  waste4desc:         { row: 30, col: 9 },
  // Box 10 - Containers (number + type)
  waste1containerNum: { row: 21, col: 56 },
  waste1container:    { row: 21, col: 61 },
  waste2containerNum: { row: 24, col: 56 },
  waste2container:    { row: 24, col: 61 },
  waste3containerNum: { row: 27, col: 56 },
  waste3container:    { row: 27, col: 61 },
  waste4containerNum: { row: 30, col: 56 },
  waste4container:    { row: 30, col: 61 },
  // Box 11 - Quantity
  waste1qty:          { row: 21, col: 66 },
  waste2qty:          { row: 24, col: 66 },
  waste3qty:          { row: 27, col: 66 },
  waste4qty:          { row: 30, col: 66 },
  // Box 12 - Unit
  waste1uom:          { row: 21, col: 73 },
  waste2uom:          { row: 24, col: 73 },
  waste3uom:          { row: 27, col: 73 },
  waste4uom:          { row: 30, col: 73 },
  // Box 13 - Waste Codes (6 per line: 3 on row 1, 3 on row 2)
  waste1wc1:          { row: 21, col: 76 },
  waste1wc2:          { row: 21, col: 81 },
  waste1wc3:          { row: 21, col: 86 },
  waste1wc4:          { row: 22, col: 76 },
  waste1wc5:          { row: 22, col: 81 },
  waste1wc6:          { row: 22, col: 86 },
  waste2wc1:          { row: 24, col: 76 },
  waste2wc2:          { row: 24, col: 81 },
  waste2wc3:          { row: 24, col: 86 },
  waste2wc4:          { row: 25, col: 76 },
  waste2wc5:          { row: 25, col: 81 },
  waste2wc6:          { row: 25, col: 86 },
  waste3wc1:          { row: 27, col: 76 },
  waste3wc2:          { row: 27, col: 81 },
  waste3wc3:          { row: 27, col: 86 },
  waste3wc4:          { row: 28, col: 76 },
  waste3wc5:          { row: 28, col: 81 },
  waste3wc6:          { row: 28, col: 86 },
  waste4wc1:          { row: 30, col: 76 },
  waste4wc2:          { row: 30, col: 81 },
  waste4wc3:          { row: 30, col: 86 },
  waste4wc4:          { row: 31, col: 76 },
  waste4wc5:          { row: 31, col: 81 },
  waste4wc6:          { row: 31, col: 86 },
  // Box 14 - Special Handling (3 lines, MIS permanent on line 3)
  specialHandling:    { row: 33, col: 8 },
  specialHandling2:   { row: 34, col: 8 },
  specialHandling3:   { row: 35, col: 8 },
  // Box 15 - Generator Certification
  generatorCertName:  { row: 38, col: 8 }
};

// EPA Form 8700-22A Continuation Sheet MAP
// Same column positions as main form, different row layout
// Estimated positions - to be calibrated with actual form
var FORM_8700_22A_MAP = {
  // Box 21 - Generator's US EPA ID Number
  generatorEpaId:           { row: 2, col: 18 },
  // Box 22 - Page __ of __
  page:                     { row: 2, col: 40 },
  totalPages:               { row: 2, col: 43 },
  // Box 23 - Manifest Tracking Number
  manifestTrackingNum:      { row: 2, col: 55 },
  // Box 24 - Generator's Name
  generatorName:            { row: 4, col: 8 },
  // Box 25 - Transporter Company Name & EPA ID
  contTransporterName:      { row: 6, col: 8 },
  contTransporterEpaId:     { row: 6, col: 62 },
  // Box 26 - Transporter 2 Company Name & EPA ID
  contTransporter2Name:     { row: 8, col: 8 },
  contTransporter2EpaId:    { row: 8, col: 62 },
  // Box 32 - Special Handling Instructions
  specialHandling:          { row: 50, col: 8 },
  specialHandling2:         { row: 51, col: 8 },
  specialHandling3:         { row: 52, col: 8 },
  // Box 33 - Transporter Acknowledgment of Receipt
  contTransporterPrintName: { row: 54, col: 8 },
  contTransporterDate:      { row: 54, col: 62 },
  // Box 34 - Transporter 2 Acknowledgment
  contTransporter2PrintName:{ row: 56, col: 8 },
  contTransporter2Date:     { row: 56, col: 62 },
  // Box 35 - Discrepancy
  contDiscrepancyInfo:      { row: 58, col: 8 }
};
// Continuation sheet waste lines: 10 lines, starting row 11, 3 rows apart
var CONT_WASTE_START_ROW = 11;
var CONT_WASTE_ROW_SPACING = 3;
var CONT_MAX_WASTE_LINES = 10;

// Print manifest - plain text for dot matrix
// Epson LQ-590II at 12 CPI, tractor feed locked all the way left
// Pinfeed manifests with strips on left and right sides (~0.5" each = ~6 chars at 12 CPI)
// MAP column values already account for the left pinfeed strip offset
var BUILD_VERSION = 'v33-2026-03-08';
app.get('/api/version', function(req, res) { res.json({ version: BUILD_VERSION }); });

// Alignment system - clean slate for v26
// colShift: positive moves text RIGHT, negative moves text LEFT (global fine-tune)
var customAlignment = data.customAlignment || null;
var previousAlignment = data.previousAlignment || null;
var colShift = (typeof data.colShift === 'number') ? data.colShift : 0;
var rowShift = (typeof data.rowShift === 'number') ? data.rowShift : 0;

// V26 migration: complete alignment reset for Epson LQ-590II pinfeed setup
if (!data.migratedToV26) {
  customAlignment = null;
  delete data.customAlignment;
  previousAlignment = null;
  delete data.previousAlignment;
  colShift = 0;
  data.colShift = 0;
  rowShift = 0;
  data.rowShift = 0;
  // Clean up old alignment variables
  delete data.colOffset;
  delete data.leftMargin;
  data.migratedToV26 = true;
  saveData(data);
  console.log('V26 migration: complete alignment reset for Epson LQ-590II pinfeed');
}

function getActiveMap() {
  if (!customAlignment) return FORM_8700_MAP;
  var merged = {};
  var keys = Object.keys(FORM_8700_MAP);
  for (var i = 0; i < keys.length; i++) {
    if (customAlignment[keys[i]]) {
      merged[keys[i]] = customAlignment[keys[i]];
    } else {
      merged[keys[i]] = FORM_8700_MAP[keys[i]];
    }
  }
  return merged;
}

app.get('/api/alignment', function(req, res) {
  res.json({
    map: getActiveMap(),
    defaults: FORM_8700_MAP,
    hasPrevious: previousAlignment !== null,
    colShift: colShift,
    rowShift: rowShift
  });
});

app.put('/api/alignment', function(req, res) {
  previousAlignment = customAlignment ? JSON.parse(JSON.stringify(customAlignment)) : null;
  data.previousAlignment = previousAlignment;
  customAlignment = req.body.map || null;
  data.customAlignment = customAlignment;
  if (typeof req.body.colShift === 'number') {
    colShift = req.body.colShift;
    data.colShift = colShift;
  }
  if (typeof req.body.rowShift === 'number') {
    rowShift = req.body.rowShift;
    data.rowShift = rowShift;
  }
  saveData(data);
  res.json({ ok: true });
});

app.post('/api/alignment/reset', function(req, res) {
  previousAlignment = customAlignment ? JSON.parse(JSON.stringify(customAlignment)) : null;
  data.previousAlignment = previousAlignment;
  customAlignment = null;
  delete data.customAlignment;
  colShift = 0;
  data.colShift = 0;
  rowShift = 0;
  data.rowShift = 0;
  saveData(data);
  res.json({ ok: true });
});

app.post('/api/alignment/undo', function(req, res) {
  if (previousAlignment === null) {
    return res.json({ ok: false, message: 'No previous settings to restore' });
  }
  var temp = customAlignment ? JSON.parse(JSON.stringify(customAlignment)) : null;
  customAlignment = JSON.parse(JSON.stringify(previousAlignment));
  previousAlignment = temp;
  data.customAlignment = customAlignment;
  data.previousAlignment = previousAlignment;
  saveData(data);
  res.json({ ok: true, map: getActiveMap() });
});

// Alignment test print - prints a grid pattern to calibrate field positions
app.get('/api/print/alignment-test', function(req, res) {
  var pageLines = [];
  for (var l = 0; l < 66; l++) {
    var row = '';
    for (var c = 0; c < 132; c++) { row += ' '; }
    pageLines.push(row);
  }

  function testPlace(row, col, text) {
    if (!text) return;
    text = String(text);
    var r = row + rowShift;
    var c = col + colShift;
    if (r < 1 || r > 66 || c < 1) return;
    var line = pageLines[r - 1];
    var before = line.substring(0, c - 1);
    var after = line.substring(c - 1 + text.length);
    pageLines[r - 1] = before + text + after;
  }

  // Header
  testPlace(1, 1, 'ALIGNMENT TEST - Epson LQ-590II 12CPI Pinfeed - ' + BUILD_VERSION);
  testPlace(2, 1, 'ColShift=' + colShift + ' RowShift=' + rowShift);

  // Column ruler every 10 rows
  var ruler = '';
  for (var ri = 1; ri <= 100; ri++) {
    if (ri % 10 === 0) { ruler += String(ri / 10); }
    else if (ri % 5 === 0) { ruler += '+'; }
    else { ruler += '.'; }
  }
  testPlace(3, 1, 'COL:' + ruler);

  // Show where each field would print using current MAP
  var MAP = getActiveMap();
  testPlace(5, 1, '--- FIELD POSITIONS (current MAP) ---');
  testPlace(MAP.generatorEpaId.row, MAP.generatorEpaId.col, '[BOX1:GenEPAID]');
  testPlace(MAP.page.row, MAP.page.col, '[B2:Pg]');
  testPlace(MAP.totalPages.row, MAP.totalPages.col, '[of]');
  testPlace(MAP.emergencyPhone.row, MAP.emergencyPhone.col, '[BOX3:EmergPh]');
  testPlace(MAP.generatorName.row, MAP.generatorName.col, '[BOX5:GenName________]');
  testPlace(MAP.generatorMailAddr.row, MAP.generatorMailAddr.col, '[MailAddr__________]');
  testPlace(MAP.generatorMailCity.row, MAP.generatorMailCity.col, '[MailCity___]');
  testPlace(MAP.generatorPhone.row, MAP.generatorPhone.col, '[GenPhone___]');
  testPlace(MAP.generatorSiteAddr.row, MAP.generatorSiteAddr.col, '[SiteAddr__________]');
  testPlace(MAP.generatorSiteCity.row, MAP.generatorSiteCity.col, '[SiteCity___]');
  testPlace(MAP.transporter1Name.row, MAP.transporter1Name.col, '[BOX6:Trans1Name_________]');
  testPlace(MAP.transporter1EpaId.row, MAP.transporter1EpaId.col, '[Trans1EPAID___]');
  testPlace(MAP.transporter2Name.row, MAP.transporter2Name.col, '[BOX7:Trans2Name_________]');
  testPlace(MAP.transporter2EpaId.row, MAP.transporter2EpaId.col, '[Trans2EPAID___]');
  testPlace(MAP.facilityName.row, MAP.facilityName.col, '[BOX8:FacName___________]');
  testPlace(MAP.facilityEpaId.row, MAP.facilityEpaId.col, '[FacEPAID______]');
  testPlace(MAP.facilityAddress.row, MAP.facilityAddress.col, '[FacAddr___________]');
  testPlace(MAP.facilityPhone.row, MAP.facilityPhone.col, '[FacPh_]');
  testPlace(MAP.facilityCity.row, MAP.facilityCity.col, '[FacCity____]');

  // Waste line 1 markers
  testPlace(MAP.waste1hm.row, MAP.waste1hm.col, '[HM]');
  testPlace(MAP.waste1desc.row, MAP.waste1desc.col, '[BOX9b:WasteDescription1_________________]');
  testPlace(MAP.waste1containerNum.row, MAP.waste1containerNum.col, '[#Cn]');
  testPlace(MAP.waste1container.row, MAP.waste1container.col, '[Typ]');
  testPlace(MAP.waste1qty.row, MAP.waste1qty.col, '[Qty__]');
  testPlace(MAP.waste1uom.row, MAP.waste1uom.col, '[U]');
  testPlace(MAP.waste1wc1.row, MAP.waste1wc1.col, '[WC1][WC2][WC3]');

  // Waste line 2 markers
  testPlace(MAP.waste2hm.row, MAP.waste2hm.col, '[HM]');
  testPlace(MAP.waste2desc.row, MAP.waste2desc.col, '[BOX9b:WasteDescription2_________________]');

  // Waste line 3 markers
  testPlace(MAP.waste3hm.row, MAP.waste3hm.col, '[HM]');
  testPlace(MAP.waste3desc.row, MAP.waste3desc.col, '[BOX9b:WasteDescription3_________________]');

  // Waste line 4 markers
  testPlace(MAP.waste4hm.row, MAP.waste4hm.col, '[HM]');
  testPlace(MAP.waste4desc.row, MAP.waste4desc.col, '[BOX9b:WasteDescription4_________________]');

  // Box 14
  testPlace(MAP.specialHandling.row, MAP.specialHandling.col, '[BOX14:SpecialHandling__________________]');
  testPlace(MAP.specialHandling2.row, MAP.specialHandling2.col, '[SpecialHandling2____________________]');

  // Box 15
  testPlace(MAP.generatorCertName.row, MAP.generatorCertName.col, '[BOX15:GenCertName______]');

  // Row numbers on left edge
  for (var rn = 1; rn <= 66; rn++) {
    var rnStr = rn < 10 ? ' ' + rn : String(rn);
    // Place at col 1 (may overlap pinfeed strip - that's fine for test)
    var line = pageLines[rn - 1];
    pageLines[rn - 1] = rnStr + line.substring(2);
  }

  var testOutput = pageLines.join('\n');
  var testHtml = '<!DOCTYPE html><html><head><title>Alignment Test</title><style>';
  testHtml += '@media print { @page { margin: 0; size: auto; } html, body { margin: 0; padding: 0; } body { font-family: monospace; font-size: 10pt; line-height: 1; white-space: pre; } }';
  testHtml += '@media screen { body { font-family: monospace; font-size: 10pt; line-height: 1; white-space: pre; margin: 20px; } }';
  testHtml += '</style></head><body>';
  testHtml += testOutput.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  testHtml += '</body></html>';
  res.set('Content-Type', 'text/html');
  res.send(testHtml);
});

app.get('/api/print/manifest/:id', function(req, res) {
  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).send('Manifest not found');

  var MAP = getActiveMap();

  // Helper: create page canvas
  // Use 60 lines instead of 66 to prevent browser print dialog from pushing to a 2nd page
  // (browser adds headers/footers/margins that eat ~6 lines of space)
  var CANVAS_ROWS = 60;
  function createCanvas() {
    var pageLines = [];
    for (var l = 0; l < CANVAS_ROWS; l++) {
      var row = '';
      for (var c = 0; c < 132; c++) { row += ' '; }
      pageLines.push(row);
    }
    return pageLines;
  }

  // Helper: place text on a canvas
  // colShift/rowShift provide global fine-tuning (positive = right/down, negative = left/up)
  function placeText(pageLines, row, col, text) {
    if (!text) return;
    text = String(text);
    var actualRow = row + rowShift;
    var actualCol = col + colShift;
    if (actualRow < 1 || actualRow > CANVAS_ROWS) return;
    if (actualCol < 1) return;
    var line = pageLines[actualRow - 1];
    var before = line.substring(0, actualCol - 1);
    var after = line.substring(actualCol - 1 + text.length);
    pageLines[actualRow - 1] = before + text + after;
  }

  // Helper: wrap description text
  function wrapDescLines(text, firstMax, contMax) {
    if (!text) return [];
    var result = [];
    var remaining = String(text);
    if (remaining.length <= firstMax) { return [remaining]; }
    var cut = remaining.lastIndexOf(' ', firstMax);
    if (cut <= 0) cut = firstMax;
    result.push(remaining.substring(0, cut));
    remaining = remaining.substring(cut).replace(/^\s+/, '');
    while (remaining.length > 0) {
      if (remaining.length <= contMax) { result.push(remaining); break; }
      cut = remaining.lastIndexOf(' ', contMax);
      if (cut <= 0) cut = contMax;
      result.push(remaining.substring(0, cut));
      remaining = remaining.substring(cut).replace(/^\s+/, '');
    }
    return result;
  }

  // Helper: smart parse waste codes
  function parseWasteCodes(allCodes) {
    if (!allCodes) return [];
    var codeArr = allCodes.split(/[\s,]+/).filter(function(c) { return c.length > 0; });
    var needsSmart = false;
    for (var sc = 0; sc < codeArr.length; sc++) {
      if (codeArr[sc].length > 4) { needsSmart = true; break; }
    }
    if (needsSmart) {
      var smartCodes = [];
      var joined = allCodes.replace(/[\s,]+/g, '');
      var letterRe = /[A-Za-z]\d{3}/g;
      var lm;
      var positions = [];
      while ((lm = letterRe.exec(joined)) !== null) {
        positions.push({start: lm.index, end: lm.index + lm[0].length, code: lm[0]});
      }
      var lastEnd = 0;
      for (var pi = 0; pi < positions.length; pi++) {
        var gap = joined.substring(lastEnd, positions[pi].start);
        if (gap.length > 0) {
          var gapNums = gap.match(/\d{3}/g);
          if (gapNums) { for (var gi = 0; gi < gapNums.length; gi++) smartCodes.push(gapNums[gi]); }
        }
        smartCodes.push(positions[pi].code);
        lastEnd = positions[pi].end;
      }
      var trail = joined.substring(lastEnd);
      if (trail.length > 0) {
        var trailNums = trail.match(/\d{3}/g);
        if (trailNums) { for (var ti = 0; ti < trailNums.length; ti++) smartCodes.push(trailNums[ti]); }
      }
      if (smartCodes.length > 1) codeArr = smartCodes;
    }
    return codeArr;
  }

  // Helper: place a waste line on a canvas given row positions
  function placeWasteLine(pageLines, manifestLineNum, baseRow, descCol, hmCol, containerNumCol, containerCol, qtyCol, uomCol, wcCol, wcRowSpacing) {
    var w = manifestLineNum;
    var wasteDesc = manifest['waste' + w + 'Description'] || '';
    var descRow1Width = containerNumCol - descCol - 1;
    var descLines = wrapDescLines(wasteDesc, descRow1Width, 55);
    for (var dl = 0; dl < descLines.length && dl < 2; dl++) {
      placeText(pageLines, baseRow + dl, descCol, descLines[dl]);
    }
    placeText(pageLines, baseRow, hmCol, manifest['waste' + w + 'HM']);
    placeText(pageLines, baseRow, containerNumCol, manifest['waste' + w + 'ContainerNum']);
    placeText(pageLines, baseRow, containerCol, manifest['waste' + w + 'ContainerType']);
    placeText(pageLines, baseRow, qtyCol, manifest['waste' + w + 'Qty']);
    placeText(pageLines, baseRow, uomCol, manifest['waste' + w + 'Unit']);
    var codeArr = parseWasteCodes((manifest['waste' + w + 'WasteCodes'] || '').trim());
    for (var ci = 0; ci < 6 && ci < codeArr.length; ci++) {
      var wcRow = baseRow + (ci >= 3 ? wcRowSpacing : 0);
      var wcColOffset = (ci % 3) * 5;
      placeText(pageLines, wcRow, wcCol + wcColOffset, codeArr[ci]);
    }
  }

  // Calculate total waste lines and pages
  // Re-count active lines at print time (ignore empty padding from old saves)
  // A line is "active" only if it has a real description (not just "RQ, " prefix), waste codes, or quantity
  var rawWLC = parseInt(manifest.wasteLineCount) || 4;
  var wasteLineCount = 0;
  for (var wlCheck = 1; wlCheck <= Math.max(rawWLC, 4); wlCheck++) {
    var wDesc = (manifest['waste' + wlCheck + 'Description'] || '').replace(/^RQ,?\s*/i, '').trim();
    var wCodes = (manifest['waste' + wlCheck + 'WasteCodes'] || '').trim();
    var wQty = (manifest['waste' + wlCheck + 'Qty'] || '').trim();
    if (wDesc || wCodes || wQty) {
      wasteLineCount = wlCheck;
    }
  }
  if (wasteLineCount < 4) wasteLineCount = 4; // minimum 4 on main form
  console.log('Print manifest ' + manifest.id + ': rawWLC=' + rawWLC + ', activeLines=' + wasteLineCount);
  var totalPages = wasteLineCount <= 4 ? 1 : Math.ceil((wasteLineCount - 4) / CONT_MAX_WASTE_LINES) + 1;
  console.log('Print manifest ' + manifest.id + ': totalPages=' + totalPages);

  // Build Box 14 auto-populate across ALL waste lines
  var autoLine1 = '';
  if (!manifest.specialHandling || manifest.specialHandling.trim() === '') {
    var parts14 = [];
    for (var b14 = 1; b14 <= wasteLineCount; b14++) {
      var pid14 = manifest['waste' + b14 + 'ProfileId'] || '';
      var ctype14 = manifest['waste' + b14 + 'ContainerType'] || '';
      var csize14 = manifest['waste' + b14 + 'ContainerSize'] || '';
      var desc14 = manifest['waste' + b14 + 'Description'] || '';
      if (!desc14 && !pid14) continue;
      var entry = '';
      if (pid14) entry += pid14;
      if (csize14) entry += (entry ? ' ' : '') + csize14;
      if (ctype14) entry += (entry ? ' ' : '') + ctype14;
      if (entry) parts14.push('9b.' + b14 + '= ' + entry);
    }
    autoLine1 = parts14.join(', ');
  }
  var sh1 = manifest.specialHandling || autoLine1;
  var sh2 = manifest.specialHandling2 || '';
  var sh3 = manifest.specialHandling3 || '';

  // ===== PAGE 1: Main Form (8700-22) =====
  var page1 = createCanvas();

  // Box 1 - Generator EPA ID
  placeText(page1, MAP.generatorEpaId.row, MAP.generatorEpaId.col, manifest.generatorEpaId);
  // Box 2 - Page
  placeText(page1, MAP.page.row, MAP.page.col, '1');
  placeText(page1, MAP.totalPages.row, MAP.totalPages.col, String(totalPages));
  // Box 3 - Emergency Response Phone
  placeText(page1, MAP.emergencyPhone.row, MAP.emergencyPhone.col, manifest.emergencyPhone);
  // Box 5 - Generator
  placeText(page1, MAP.generatorName.row, MAP.generatorName.col, manifest.generatorName);
  placeText(page1, MAP.generatorPhone.row, MAP.generatorPhone.col, manifest.generatorPhone);
  placeText(page1, MAP.generatorMailAddr.row, MAP.generatorMailAddr.col, manifest.generatorAddress);
  placeText(page1, MAP.generatorMailCity.row, MAP.generatorMailCity.col, manifest.generatorCityStZip);
  placeText(page1, MAP.generatorSiteAddr.row, MAP.generatorSiteAddr.col, manifest.genSiteAddress);
  placeText(page1, MAP.generatorSiteCity.row, MAP.generatorSiteCity.col, manifest.genSiteCityStZip);
  // Box 6 - Transporter 1
  placeText(page1, MAP.transporter1Name.row, MAP.transporter1Name.col, manifest.transporter1Name);
  placeText(page1, MAP.transporter1EpaId.row, MAP.transporter1EpaId.col, manifest.transporter1EpaId);
  // Box 7 - Transporter 2
  placeText(page1, MAP.transporter2Name.row, MAP.transporter2Name.col, manifest.transporter2Name);
  placeText(page1, MAP.transporter2EpaId.row, MAP.transporter2EpaId.col, manifest.transporter2EpaId);
  // Box 8 - Facility
  placeText(page1, MAP.facilityName.row, MAP.facilityName.col, manifest.facilityName);
  placeText(page1, MAP.facilityPhone.row, MAP.facilityPhone.col, manifest.facilityPhone);
  placeText(page1, MAP.facilityAddress.row, MAP.facilityAddress.col, manifest.facilityAddress);
  placeText(page1, MAP.facilityCity.row, MAP.facilityCity.col, manifest.facilityCityStZip);
  placeText(page1, MAP.facilityEpaId.row, MAP.facilityEpaId.col, manifest.facilityEpaId);

  // Box 9-13 - Waste lines 1-4 on main form
  for (var w = 1; w <= 4; w++) {
    var wasteDesc = manifest['waste' + w + 'Description'] || '';
    var descRow1Width = MAP.waste1containerNum.col - MAP.waste1desc.col - 1;
    var descLines = wrapDescLines(wasteDesc, descRow1Width, 55);
    var descRow = MAP['waste' + w + 'desc'].row;
    for (var dl = 0; dl < descLines.length && dl < 3; dl++) {
      placeText(page1, descRow + dl, MAP['waste' + w + 'desc'].col, descLines[dl]);
    }
    placeText(page1, MAP['waste' + w + 'hm'].row, MAP['waste' + w + 'hm'].col, manifest['waste' + w + 'HM']);
    placeText(page1, MAP['waste' + w + 'containerNum'].row, MAP['waste' + w + 'containerNum'].col, manifest['waste' + w + 'ContainerNum']);
    placeText(page1, MAP['waste' + w + 'container'].row, MAP['waste' + w + 'container'].col, manifest['waste' + w + 'ContainerType']);
    placeText(page1, MAP['waste' + w + 'qty'].row, MAP['waste' + w + 'qty'].col, manifest['waste' + w + 'Qty']);
    placeText(page1, MAP['waste' + w + 'uom'].row, MAP['waste' + w + 'uom'].col, manifest['waste' + w + 'Unit']);
    var allCodes = (manifest['waste' + w + 'WasteCodes'] || '').trim();
    if (allCodes) {
      var codeArr = parseWasteCodes(allCodes);
      for (var ci = 0; ci < 6; ci++) {
        if (codeArr[ci]) {
          var wcKey = 'waste' + w + 'wc' + (ci + 1);
          if (MAP[wcKey]) {
            placeText(page1, MAP[wcKey].row, MAP[wcKey].col, codeArr[ci]);
          }
        }
      }
    }
  }

  // Box 14 - Special Handling
  placeText(page1, MAP.specialHandling.row, MAP.specialHandling.col, sh1);
  placeText(page1, MAP.specialHandling2.row, MAP.specialHandling2.col, sh2);
  placeText(page1, MAP.specialHandling3.row, MAP.specialHandling3.col, sh3);
  // Box 15 - Generator Certification
  placeText(page1, MAP.generatorCertName.row, MAP.generatorCertName.col, manifest.generatorPrintName);

  var allPages = [page1.join('\n')];

  // ===== CONTINUATION PAGES (8700-22A) =====
  // Only generate continuation pages when there are more than 4 waste lines
  if (wasteLineCount > 4) {
    var contMap = FORM_8700_22A_MAP;
    var remainingLines = wasteLineCount - 4;
    var contPageNum = 2;
    var manifestLineStart = 5;
    var contPageCount = Math.ceil(remainingLines / CONT_MAX_WASTE_LINES);

    for (var cpIdx = 0; cpIdx < contPageCount; cpIdx++) {
      var linesOnThisPage = Math.min(remainingLines, CONT_MAX_WASTE_LINES);
      var contPage = createCanvas();

      // Box 21 - Generator EPA ID
      placeText(contPage, contMap.generatorEpaId.row, contMap.generatorEpaId.col, manifest.generatorEpaId);
      // Box 22 - Page
      placeText(contPage, contMap.page.row, contMap.page.col, manifest.contPageNum || String(contPageNum));
      placeText(contPage, contMap.totalPages.row, contMap.totalPages.col, String(totalPages));
      // Box 23 - Manifest Tracking Number
      placeText(contPage, contMap.manifestTrackingNum.row, contMap.manifestTrackingNum.col, manifest.manifestTrackingNum);
      // Box 24 - Generator Name
      placeText(contPage, contMap.generatorName.row, contMap.generatorName.col, manifest.generatorName);
      // Box 25 - Transporter
      placeText(contPage, contMap.contTransporterName.row, contMap.contTransporterName.col, manifest.contTransporterName || manifest.transporter1Name);
      placeText(contPage, contMap.contTransporterEpaId.row, contMap.contTransporterEpaId.col, manifest.contTransporterEpaId || manifest.transporter1EpaId);
      // Box 26 - Transporter 2
      placeText(contPage, contMap.contTransporter2Name.row, contMap.contTransporter2Name.col, manifest.contTransporter2Name);
      placeText(contPage, contMap.contTransporter2EpaId.row, contMap.contTransporter2EpaId.col, manifest.contTransporter2EpaId);

      // Box 27-31 - Waste lines on this continuation page
      for (var cw = 0; cw < linesOnThisPage; cw++) {
        var mLineNum = manifestLineStart + cw;
        var contRow = CONT_WASTE_START_ROW + (cw * CONT_WASTE_ROW_SPACING);
        placeWasteLine(contPage, mLineNum, contRow,
          MAP.waste1desc.col, MAP.waste1hm.col,
          MAP.waste1containerNum.col, MAP.waste1container.col,
          MAP.waste1qty.col, MAP.waste1uom.col,
          MAP.waste1wc1.col, 1);
      }

      // Box 32 - Special Handling
      var contSh1 = manifest.contSpecialHandling || sh1;
      var contSh2 = manifest.contSpecialHandling2 || sh2;
      var contSh3 = manifest.contSpecialHandling3 || sh3;
      placeText(contPage, contMap.specialHandling.row, contMap.specialHandling.col, contSh1);
      placeText(contPage, contMap.specialHandling2.row, contMap.specialHandling2.col, contSh2);
      placeText(contPage, contMap.specialHandling3.row, contMap.specialHandling3.col, contSh3);
      // Box 33 - Transporter Acknowledgment
      placeText(contPage, contMap.contTransporterPrintName.row, contMap.contTransporterPrintName.col, manifest.contTransporterPrintName);
      placeText(contPage, contMap.contTransporterDate.row, contMap.contTransporterDate.col, manifest.contTransporterDate);
      // Box 34 - Transporter 2 Acknowledgment
      placeText(contPage, contMap.contTransporter2PrintName.row, contMap.contTransporter2PrintName.col, manifest.contTransporter2PrintName);
      placeText(contPage, contMap.contTransporter2Date.row, contMap.contTransporter2Date.col, manifest.contTransporter2Date);
      // Box 35 - Discrepancy
      placeText(contPage, contMap.contDiscrepancyInfo.row, contMap.contDiscrepancyInfo.col, manifest.contDiscrepancyInfo);

      allPages.push(contPage.join('\n'));
      remainingLines -= linesOnThisPage;
      manifestLineStart += linesOnThisPage;
      contPageNum++;
    }
  }

  // Support ?page=N to print a specific page, or all pages if not specified
  var requestedPage = parseInt(req.query.page);
  var output;
  if (requestedPage && requestedPage >= 1 && requestedPage <= allPages.length) {
    output = allPages[requestedPage - 1];
  } else {
    output = allPages.join('\f');
  }
  // Trim trailing blank lines
  output = output.replace(/\n\s*$/, '');
  console.log('Print output: ' + allPages.length + ' page(s), ' + output.split('\n').length + ' lines');

  // If ?raw=1, send plain text (for raw printer drivers)
  if (req.query.raw === '1') {
    res.set('Content-Type', 'text/plain');
    return res.send(output);
  }

  // Wrap in HTML with print stylesheet to eliminate browser margins/headers/footers
  // This prevents the browser from pushing content onto a 2nd page
  var htmlOutput = '<!DOCTYPE html><html><head><title>Manifest Print</title><style>';
  htmlOutput += '@media print {';
  htmlOutput += '  @page { margin: 0; size: auto; }';
  htmlOutput += '  html, body { margin: 0; padding: 0; }';
  htmlOutput += '  body { font-family: monospace; font-size: 10pt; line-height: 1; white-space: pre; }';
  htmlOutput += '}';
  htmlOutput += '@media screen {';
  htmlOutput += '  body { font-family: monospace; font-size: 10pt; line-height: 1; white-space: pre; margin: 20px; }';
  htmlOutput += '}';
  htmlOutput += '</style></head><body>';
  // Escape HTML entities and preserve whitespace
  var escaped = output.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  htmlOutput += escaped;
  htmlOutput += '</body></html>';
  res.set('Content-Type', 'text/html');
  res.send(htmlOutput);
});

// Download .prn file
app.get('/api/print/raw/:id', function(req, res) {
  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).send('Manifest not found');
  res.redirect('/api/print/manifest/' + req.params.id);
});

// Serve static files
app.use(express.static(path.join(__dirname)));

// Start server
app.listen(PORT, function() {
  console.log('Manifest Platform running on port ' + PORT);
  console.log('Data directory: ' + DATA_DIR);
});

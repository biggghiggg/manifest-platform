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
  // Epson LQ-590II, 12 CPI, tractor feed locked left, pinfeed manifests
  // Plain text output (browser adds ~5 char left margin when printing)
  // Box 1 - Generator's US EPA ID Number
  generatorEpaId:     { row: 4, col: 13 },
  // Box 2 - Page __ of __
  page:               { row: 4, col: 35 },
  totalPages:         { row: 4, col: 38 },
  // Box 3 - Emergency Response Phone
  emergencyPhone:     { row: 4, col: 42 },
  // Box 5 - Generator
  generatorName:      { row: 6, col: 11 },
  // Mailing Address (LEFT side of Box 5)
  generatorMailAddr:  { row: 7, col: 11 },
  generatorMailCity:  { row: 8, col: 11 },
  generatorPhone:     { row: 8, col: 29 },
  // Site Address (RIGHT side of Box 5)
  generatorSiteAddr:  { row: 7, col: 43 },
  generatorSiteCity:  { row: 8, col: 43 },
  // Box 6 - Transporter 1
  transporter1Name:   { row: 10, col: 3 },
  transporter1EpaId:  { row: 10, col: 57 },
  // Box 7 - Transporter 2
  transporter2Name:   { row: 12, col: 3 },
  transporter2EpaId:  { row: 12, col: 57 },
  // Box 8 - Designated Facility
  facilityName:       { row: 14, col: 3 },
  facilityEpaId:      { row: 14, col: 57 },
  facilityAddress:    { row: 15, col: 3 },
  facilityPhone:      { row: 16, col: 3 },
  facilityCity:       { row: 16, col: 19 },
  facilityState:      { row: 16, col: 33 },
  facilityZip:        { row: 16, col: 37 },
  // Box 9a - HM (col 1 = leftmost printable position, browser margin pushes it right ~5 chars)
  waste1hm:           { row: 21, col: 1 },
  waste2hm:           { row: 24, col: 1 },
  waste3hm:           { row: 27, col: 1 },
  waste4hm:           { row: 30, col: 1 },
  // Box 9b - Description
  waste1desc:         { row: 21, col: 5 },
  waste2desc:         { row: 24, col: 5 },
  waste3desc:         { row: 27, col: 5 },
  waste4desc:         { row: 30, col: 5 },
  // Box 10 - Containers (number + type)
  waste1containerNum: { row: 21, col: 51 },
  waste1container:    { row: 21, col: 56 },
  waste2containerNum: { row: 24, col: 51 },
  waste2container:    { row: 24, col: 56 },
  waste3containerNum: { row: 27, col: 51 },
  waste3container:    { row: 27, col: 56 },
  waste4containerNum: { row: 30, col: 51 },
  waste4container:    { row: 30, col: 56 },
  // Box 11 - Quantity
  waste1qty:          { row: 21, col: 61 },
  waste2qty:          { row: 24, col: 61 },
  waste3qty:          { row: 27, col: 61 },
  waste4qty:          { row: 30, col: 61 },
  // Box 12 - Unit
  waste1uom:          { row: 21, col: 68 },
  waste2uom:          { row: 24, col: 68 },
  waste3uom:          { row: 27, col: 68 },
  waste4uom:          { row: 30, col: 68 },
  // Box 13 - Waste Codes (6 per line: 3 on row 1, 3 on row 2)
  waste1wc1:          { row: 21, col: 71 },
  waste1wc2:          { row: 21, col: 76 },
  waste1wc3:          { row: 21, col: 81 },
  waste1wc4:          { row: 22, col: 71 },
  waste1wc5:          { row: 22, col: 76 },
  waste1wc6:          { row: 22, col: 81 },
  waste2wc1:          { row: 24, col: 71 },
  waste2wc2:          { row: 24, col: 76 },
  waste2wc3:          { row: 24, col: 81 },
  waste2wc4:          { row: 25, col: 71 },
  waste2wc5:          { row: 25, col: 76 },
  waste2wc6:          { row: 25, col: 81 },
  waste3wc1:          { row: 27, col: 71 },
  waste3wc2:          { row: 27, col: 76 },
  waste3wc3:          { row: 27, col: 81 },
  waste3wc4:          { row: 28, col: 71 },
  waste3wc5:          { row: 28, col: 76 },
  waste3wc6:          { row: 28, col: 81 },
  waste4wc1:          { row: 30, col: 71 },
  waste4wc2:          { row: 30, col: 76 },
  waste4wc3:          { row: 30, col: 81 },
  waste4wc4:          { row: 31, col: 71 },
  waste4wc5:          { row: 31, col: 76 },
  waste4wc6:          { row: 31, col: 81 },
  // Box 14 - Special Handling (3 lines, MIS permanent on line 3)
  specialHandling:    { row: 33, col: 3 },
  specialHandling2:   { row: 34, col: 3 },
  specialHandling3:   { row: 35, col: 3 },
  // Box 15 - Generator Certification
  generatorCertName:  { row: 38, col: 3 }
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
var BUILD_VERSION = 'v51-2026-03-09';
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

// V38 migration: MAP columns shifted -10, reset colShift to 0
if (!data.migratedToV38) {
  customAlignment = null;
  delete data.customAlignment;
  previousAlignment = null;
  delete data.previousAlignment;
  colShift = 0;
  data.colShift = 0;
  rowShift = 0;
  data.rowShift = 0;
  data.migratedToV38 = true;
  saveData(data);
  console.log('V38 migration: MAP columns shifted -10, reset shifts to 0');
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
    if (r < 1 || r > 66) return;
    if (c < 1) c = 1;
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

  res.set('Content-Type', 'text/plain; charset=utf-8');
  res.send(pageLines.join('\n'));
});

app.get('/api/print/manifest/:id', function(req, res) {
  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).send('Manifest not found');

  var MAP = getActiveMap();

  // Helper: create page canvas (66 lines = 11" at 6 LPI)
  var CANVAS_ROWS = 66;
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
  // colShift/rowShift provide small fine-tuning (positive = right/down, negative = left/up)
  // MAP positions already account for printer/paper setup, so colShift should stay near 0
  function placeText(pageLines, row, col, text) {
    if (!text) return;
    text = String(text);
    var actualRow = row + rowShift;
    var actualCol = col + colShift;
    if (actualRow < 1 || actualRow > CANVAS_ROWS) return;
    if (actualCol < 1) actualCol = 1; // safety: print at col 1 instead of skipping
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
  // Box 2 - Page (only print if user manually entered values)
  if (manifest.pageNum) placeText(page1, MAP.page.row, MAP.page.col, manifest.pageNum);
  if (manifest.pageTotal) placeText(page1, MAP.totalPages.row, MAP.totalPages.col, manifest.pageTotal);
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

  // Trim trailing blank lines from page to prevent browser from pushing to 2nd page
  function trimPage(pageLines) {
    var lastNonBlank = pageLines.length - 1;
    while (lastNonBlank > 0 && pageLines[lastNonBlank].trim() === '') {
      lastNonBlank--;
    }
    return pageLines.slice(0, lastNonBlank + 1).join('\n');
  }

  var allPages = [trimPage(page1)];

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

      allPages.push(trimPage(contPage));
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

  res.set('Content-Type', 'text/plain');
  res.send(output);
});

// ESC/P2 raw print - generates .prn file for direct Epson LQ-590II printing
// Bypasses browser entirely - no margins, precise positioning
var RAW_MAP = {
  // Original 12 CPI positions for direct printer output (no browser margin)
  generatorEpaId:     { row: 4, col: 18 },
  page:               { row: 4, col: 40 },
  totalPages:         { row: 4, col: 43 },
  emergencyPhone:     { row: 4, col: 47 },
  generatorName:      { row: 6, col: 8 },
  generatorMailAddr:  { row: 7, col: 8 },
  generatorMailCity:  { row: 8, col: 8 },
  generatorPhone:     { row: 8, col: 34 },
  generatorSiteAddr:  { row: 7, col: 48 },
  generatorSiteCity:  { row: 8, col: 48 },
  transporter1Name:   { row: 10, col: 8 },
  transporter1EpaId:  { row: 10, col: 66 },
  transporter2Name:   { row: 12, col: 8 },
  transporter2EpaId:  { row: 12, col: 66 },
  facilityName:       { row: 14, col: 8 },
  facilityEpaId:      { row: 14, col: 66 },
  facilityAddress:    { row: 15, col: 8 },
  facilityCity:       { row: 16, col: 8 },
  facilityState:      { row: 16, col: 24 },
  facilityZip:        { row: 16, col: 28 },
  facilityPhone:      { row: 16, col: 38 },
  waste1hm:           { row: 20, col: 4 },
  waste2hm:           { row: 23, col: 4 },
  waste3hm:           { row: 26, col: 4 },
  waste4hm:           { row: 29, col: 4 },
  waste1desc:         { row: 20, col: 9 },
  waste2desc:         { row: 23, col: 9 },
  waste3desc:         { row: 26, col: 9 },
  waste4desc:         { row: 29, col: 9 },
  waste1containerNum: { row: 20, col: 56 },
  waste1container:    { row: 20, col: 61 },
  waste2containerNum: { row: 23, col: 56 },
  waste2container:    { row: 23, col: 61 },
  waste3containerNum: { row: 26, col: 56 },
  waste3container:    { row: 26, col: 61 },
  waste4containerNum: { row: 29, col: 56 },
  waste4container:    { row: 29, col: 61 },
  waste1qty:          { row: 20, col: 66 },
  waste2qty:          { row: 23, col: 66 },
  waste3qty:          { row: 26, col: 66 },
  waste4qty:          { row: 29, col: 66 },
  waste1uom:          { row: 20, col: 73 },
  waste2uom:          { row: 23, col: 73 },
  waste3uom:          { row: 26, col: 73 },
  waste4uom:          { row: 29, col: 73 },
  waste1wc1:          { row: 20, col: 79 },
  waste1wc2:          { row: 20, col: 84 },
  waste1wc3:          { row: 20, col: 89 },
  waste1wc4:          { row: 21, col: 79 },
  waste1wc5:          { row: 21, col: 84 },
  waste1wc6:          { row: 21, col: 89 },
  waste2wc1:          { row: 23, col: 79 },
  waste2wc2:          { row: 23, col: 84 },
  waste2wc3:          { row: 23, col: 89 },
  waste2wc4:          { row: 24, col: 79 },
  waste2wc5:          { row: 24, col: 84 },
  waste2wc6:          { row: 24, col: 89 },
  waste3wc1:          { row: 26, col: 79 },
  waste3wc2:          { row: 26, col: 84 },
  waste3wc3:          { row: 26, col: 89 },
  waste3wc4:          { row: 27, col: 79 },
  waste3wc5:          { row: 27, col: 84 },
  waste3wc6:          { row: 27, col: 89 },
  waste4wc1:          { row: 29, col: 79 },
  waste4wc2:          { row: 29, col: 84 },
  waste4wc3:          { row: 29, col: 89 },
  waste4wc4:          { row: 30, col: 79 },
  waste4wc5:          { row: 30, col: 84 },
  waste4wc6:          { row: 30, col: 89 },
  specialHandling:    { row: 32, col: 8 },
  specialHandling2:   { row: 33, col: 8 },
  specialHandling3:   { row: 34, col: 8 },
  generatorCertName:  { row: 38, col: 8 }
};

app.get('/api/print/escp2/:id', function(req, res) {
  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).send('Manifest not found');

  var M = RAW_MAP;
  var commands = [];

  function addBytes(arr) { commands.push(Buffer.from(arr)); }
  function addText(text) { commands.push(Buffer.from(text, 'ascii')); }

  // Position print head and print text
  // Row/col are 1-based. Horizontal: (col-1)*5 in 1/60" units. Vertical: (row-1)*60 in 1/360" units.
  function printAt(row, col, text) {
    if (!text) return;
    text = String(text);
    // ESC ( V 2 0 mL mH - absolute vertical position in 1/360"
    var vPos = (row - 1) * 60;
    var mL = vPos & 0xFF;
    var mH = (vPos >> 8) & 0xFF;
    addBytes([0x1B, 0x28, 0x56, 0x02, 0x00, mL, mH]);
    // ESC $ nL nH - absolute horizontal position in 1/60"
    var hPos = (col - 1) * 5;
    var nL = hPos & 0xFF;
    var nH = (hPos >> 8) & 0xFF;
    addBytes([0x1B, 0x24, nL, nH]);
    // Print the text
    addText(text);
  }

  // Smart parse waste codes (reuse existing logic)
  function parseWC(allCodes) {
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

  // Wrap description text
  function wrapDesc(text, maxFirst, maxCont) {
    if (!text) return [];
    var remaining = String(text);
    if (remaining.length <= maxFirst) return [remaining];
    var result = [];
    var cut = remaining.lastIndexOf(' ', maxFirst);
    if (cut <= 0) cut = maxFirst;
    result.push(remaining.substring(0, cut));
    remaining = remaining.substring(cut).replace(/^\s+/, '');
    while (remaining.length > 0) {
      if (remaining.length <= maxCont) { result.push(remaining); break; }
      cut = remaining.lastIndexOf(' ', maxCont);
      if (cut <= 0) cut = maxCont;
      result.push(remaining.substring(0, cut));
      remaining = remaining.substring(cut).replace(/^\s+/, '');
    }
    return result;
  }

  // === Initialize printer ===
  addBytes([0x1B, 0x40]); // ESC @ - Initialize/reset
  addBytes([0x1B, 0x4D]); // ESC M - Select 12 CPI (Elite)
  addBytes([0x1B, 0x32]); // ESC 2 - Set 1/6" line spacing (6 LPI)

  // === Page 1 - Main Form (8700-22) ===

  // Count active waste lines
  var rawWLC = parseInt(manifest.wasteLineCount) || 4;
  var wasteLineCount = 0;
  for (var wlc = 1; wlc <= Math.max(rawWLC, 4); wlc++) {
    var wd = (manifest['waste' + wlc + 'Description'] || '').replace(/^RQ,?\s*/i, '').trim();
    var wco = (manifest['waste' + wlc + 'WasteCodes'] || '').trim();
    var wq = (manifest['waste' + wlc + 'Qty'] || '').trim();
    if (wd || wco || wq) wasteLineCount = wlc;
  }
  if (wasteLineCount < 4) wasteLineCount = 4;
  var totalPages = wasteLineCount <= 4 ? 1 : Math.ceil((wasteLineCount - 4) / CONT_MAX_WASTE_LINES) + 1;

  // Box 1 - Generator EPA ID
  printAt(M.generatorEpaId.row, M.generatorEpaId.col, manifest.generatorEpaId);
  // Box 2 - Page (only if user filled it in)
  if (manifest.pageNum) printAt(M.page.row, M.page.col, manifest.pageNum);
  if (manifest.pageTotal) printAt(M.totalPages.row, M.totalPages.col, manifest.pageTotal);
  // Box 3 - Emergency Response Phone
  printAt(M.emergencyPhone.row, M.emergencyPhone.col, manifest.emergencyPhone);
  // Box 5 - Generator
  printAt(M.generatorName.row, M.generatorName.col, manifest.generatorName);
  printAt(M.generatorPhone.row, M.generatorPhone.col, manifest.generatorPhone);
  printAt(M.generatorMailAddr.row, M.generatorMailAddr.col, manifest.generatorAddress);
  printAt(M.generatorMailCity.row, M.generatorMailCity.col, manifest.generatorCityStZip);
  printAt(M.generatorSiteAddr.row, M.generatorSiteAddr.col, manifest.genSiteAddress);
  printAt(M.generatorSiteCity.row, M.generatorSiteCity.col, manifest.genSiteCityStZip);
  // Box 6 - Transporter 1
  printAt(M.transporter1Name.row, M.transporter1Name.col, manifest.transporter1Name);
  printAt(M.transporter1EpaId.row, M.transporter1EpaId.col, manifest.transporter1EpaId);
  // Box 7 - Transporter 2
  printAt(M.transporter2Name.row, M.transporter2Name.col, manifest.transporter2Name);
  printAt(M.transporter2EpaId.row, M.transporter2EpaId.col, manifest.transporter2EpaId);
  // Box 8 - Designated Facility
  printAt(M.facilityName.row, M.facilityName.col, manifest.facilityName);
  printAt(M.facilityEpaId.row, M.facilityEpaId.col, manifest.facilityEpaId);
  printAt(M.facilityAddress.row, M.facilityAddress.col, manifest.facilityAddress);
  printAt(M.facilityPhone.row, M.facilityPhone.col, manifest.facilityPhone);
  printAt(M.facilityCity.row, M.facilityCity.col, manifest.facilityCityStZip);

  // Boxes 9-13 - Waste Lines (1-4 on main form)
  var maxOnPage1 = Math.min(wasteLineCount, 4);
  for (var w = 1; w <= maxOnPage1; w++) {
    var hmKey = 'waste' + w + 'hm';
    var descKey = 'waste' + w + 'desc';
    var baseRow = M[hmKey].row;

    // Box 9a - HM
    printAt(M[hmKey].row, M[hmKey].col, manifest['waste' + w + 'HM']);
    // Box 9b - Description (with word wrap)
    var descText = manifest['waste' + w + 'Description'] || '';
    var descMaxFirst = M['waste' + w + 'containerNum'].col - M[descKey].col - 1;
    var descLines = wrapDesc(descText, descMaxFirst, 55);
    for (var dl = 0; dl < descLines.length && dl < 2; dl++) {
      printAt(baseRow + dl, M[descKey].col, descLines[dl]);
    }
    // Box 10 - Containers
    printAt(baseRow, M['waste' + w + 'containerNum'].col, manifest['waste' + w + 'ContainerNum']);
    printAt(baseRow, M['waste' + w + 'container'].col, manifest['waste' + w + 'ContainerType']);
    // Box 11 - Qty
    printAt(baseRow, M['waste' + w + 'qty'].col, manifest['waste' + w + 'Qty']);
    // Box 12 - Unit
    printAt(baseRow, M['waste' + w + 'uom'].col, manifest['waste' + w + 'Unit']);
    // Box 13 - Waste Codes
    var wcKey = 'waste' + w + 'wc';
    var codes = parseWC((manifest['waste' + w + 'WasteCodes'] || '').trim());
    for (var ci = 0; ci < 6 && ci < codes.length; ci++) {
      var wcField = wcKey + (ci + 1);
      if (M[wcField]) {
        printAt(M[wcField].row, M[wcField].col, codes[ci]);
      }
    }
  }

  // Box 14 - Special Handling / Box 14 auto-populate
  var sh1 = manifest.specialHandling || '';
  var sh2 = manifest.specialHandling2 || '';
  var sh3 = manifest.specialHandling3 || '';
  if (!sh1 && !sh2) {
    // Auto-populate from waste lines
    var parts14 = [];
    for (var b14 = 1; b14 <= wasteLineCount; b14++) {
      var pid14 = manifest['waste' + b14 + 'ProfileId'] || '';
      var csize14 = manifest['waste' + b14 + 'ContainerSize'] || '';
      var ctype14 = manifest['waste' + b14 + 'ContainerType'] || '';
      var desc14 = manifest['waste' + b14 + 'Description'] || '';
      if (!desc14 && !pid14) continue;
      var label14 = '9b.' + b14 + '= ';
      if (pid14) label14 += pid14;
      if (csize14) label14 += ' ' + csize14;
      if (ctype14) label14 += ' ' + ctype14;
      parts14.push(label14.trim());
    }
    var autoText = parts14.join(', ');
    if (autoText.length > 75) {
      sh1 = autoText.substring(0, 75);
      var rest = autoText.substring(75);
      if (rest.length > 75) {
        sh2 = rest.substring(0, 75);
      } else {
        sh2 = rest;
      }
    } else {
      sh1 = autoText;
    }
  }
  printAt(M.specialHandling.row, M.specialHandling.col, sh1);
  printAt(M.specialHandling2.row, M.specialHandling2.col, sh2);
  printAt(M.specialHandling3.row, M.specialHandling3.col, sh3);

  // Box 15 - Generator Certification
  printAt(M.generatorCertName.row, M.generatorCertName.col, manifest.generatorPrintName);

  // Form feed to eject page 1
  addBytes([0x0C]);

  // === Continuation Pages (8700-22A) - if needed ===
  if (wasteLineCount > 4) {
    var contMap = FORM_8700_22A_MAP;
    var remainingLines = wasteLineCount - 4;
    var contPageNum = 2;
    var manifestLineStart = 5;
    var contPageCount = Math.ceil(remainingLines / CONT_MAX_WASTE_LINES);

    for (var cpIdx = 0; cpIdx < contPageCount; cpIdx++) {
      var linesOnThisPage = Math.min(remainingLines, CONT_MAX_WASTE_LINES);

      // Re-initialize for new page
      addBytes([0x1B, 0x40]); // ESC @ reset
      addBytes([0x1B, 0x4D]); // 12 CPI
      addBytes([0x1B, 0x32]); // 6 LPI

      printAt(contMap.generatorEpaId.row, contMap.generatorEpaId.col, manifest.generatorEpaId);
      printAt(contMap.page.row, contMap.page.col, manifest.contPageNum || String(contPageNum));
      printAt(contMap.totalPages.row, contMap.totalPages.col, String(totalPages));
      printAt(contMap.manifestTrackingNum.row, contMap.manifestTrackingNum.col, manifest.manifestTrackingNum);
      printAt(contMap.generatorName.row, contMap.generatorName.col, manifest.generatorName);
      printAt(contMap.contTransporterName.row, contMap.contTransporterName.col, manifest.contTransporterName || manifest.transporter1Name);
      printAt(contMap.contTransporterEpaId.row, contMap.contTransporterEpaId.col, manifest.contTransporterEpaId || manifest.transporter1EpaId);
      printAt(contMap.contTransporter2Name.row, contMap.contTransporter2Name.col, manifest.contTransporter2Name);
      printAt(contMap.contTransporter2EpaId.row, contMap.contTransporter2EpaId.col, manifest.contTransporter2EpaId);

      // Waste lines on continuation page
      for (var cw = 0; cw < linesOnThisPage; cw++) {
        var mLineNum = manifestLineStart + cw;
        var contRow = CONT_WASTE_START_ROW + (cw * CONT_WASTE_ROW_SPACING);
        var cwDesc = manifest['waste' + mLineNum + 'Description'] || '';
        var cwDescLines = wrapDesc(cwDesc, 45, 55);
        for (var cdl = 0; cdl < cwDescLines.length && cdl < 2; cdl++) {
          printAt(contRow + cdl, M.waste1desc.col, cwDescLines[cdl]);
        }
        printAt(contRow, M.waste1hm.col, manifest['waste' + mLineNum + 'HM']);
        printAt(contRow, M.waste1containerNum.col, manifest['waste' + mLineNum + 'ContainerNum']);
        printAt(contRow, M.waste1container.col, manifest['waste' + mLineNum + 'ContainerType']);
        printAt(contRow, M.waste1qty.col, manifest['waste' + mLineNum + 'Qty']);
        printAt(contRow, M.waste1uom.col, manifest['waste' + mLineNum + 'Unit']);
        var cwCodes = parseWC((manifest['waste' + mLineNum + 'WasteCodes'] || '').trim());
        for (var cci = 0; cci < 6 && cci < cwCodes.length; cci++) {
          var cwRow = contRow + (cci >= 3 ? 1 : 0);
          var cwColOff = (cci % 3) * 5;
          printAt(cwRow, M.waste1wc1.col + cwColOff, cwCodes[cci]);
        }
      }

      // Continuation special handling
      var cSh1 = manifest.contSpecialHandling || sh1;
      var cSh2 = manifest.contSpecialHandling2 || sh2;
      var cSh3 = manifest.contSpecialHandling3 || sh3;
      printAt(contMap.specialHandling.row, contMap.specialHandling.col, cSh1);
      printAt(contMap.specialHandling2.row, contMap.specialHandling2.col, cSh2);
      printAt(contMap.specialHandling3.row, contMap.specialHandling3.col, cSh3);
      printAt(contMap.contTransporterPrintName.row, contMap.contTransporterPrintName.col, manifest.contTransporterPrintName);
      printAt(contMap.contTransporterDate.row, contMap.contTransporterDate.col, manifest.contTransporterDate);
      printAt(contMap.contTransporter2PrintName.row, contMap.contTransporter2PrintName.col, manifest.contTransporter2PrintName);
      printAt(contMap.contTransporter2Date.row, contMap.contTransporter2Date.col, manifest.contTransporter2Date);
      printAt(contMap.contDiscrepancyInfo.row, contMap.contDiscrepancyInfo.col, manifest.contDiscrepancyInfo);

      addBytes([0x0C]); // Form feed
      remainingLines -= linesOnThisPage;
      manifestLineStart += linesOnThisPage;
      contPageNum++;
    }
  }

  // Combine all buffers and send as downloadable .prn file
  var output = Buffer.concat(commands);
  var filename = 'manifest-' + (manifest.manifestTrackingNum || manifest.id) + '.prn';
  res.set('Content-Type', 'application/octet-stream');
  res.set('Content-Disposition', 'attachment; filename="' + filename + '"');
  res.send(output);
});

// === Direct Print - HTML with CSS absolute positioning ===
// Opens in new tab, user selects Epson printer and clicks Print. No file download needed.
app.get('/api/print/direct/:id', function(req, res) {
  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).send('Manifest not found');

  var M = RAW_MAP;

  // Collect all text placements: [{row, col, text, page}]
  var placements = [];
  function placeAt(row, col, text, pg) {
    if (!text) return;
    placements.push({ row: row, col: col, text: String(text), page: pg || 1 });
  }

  // Reuse word wrap
  function wrapDesc(text, maxFirst, maxCont) {
    if (!text) return [];
    var remaining = String(text);
    if (remaining.length <= maxFirst) return [remaining];
    var result = [];
    var cut = remaining.lastIndexOf(' ', maxFirst);
    if (cut <= 0) cut = maxFirst;
    result.push(remaining.substring(0, cut));
    remaining = remaining.substring(cut).replace(/^\s+/, '');
    while (remaining.length > 0) {
      if (remaining.length <= maxCont) { result.push(remaining); break; }
      cut = remaining.lastIndexOf(' ', maxCont);
      if (cut <= 0) cut = maxCont;
      result.push(remaining.substring(0, cut));
      remaining = remaining.substring(cut).replace(/^\s+/, '');
    }
    return result;
  }

  // Smart parse waste codes
  function parseWC(allCodes) {
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

  // Count active waste lines
  var rawWLC = parseInt(manifest.wasteLineCount) || 4;
  var wasteLineCount = 0;
  for (var wlc = 1; wlc <= Math.max(rawWLC, 4); wlc++) {
    var wd = (manifest['waste' + wlc + 'Description'] || '').replace(/^RQ,?\s*/i, '').trim();
    var wco = (manifest['waste' + wlc + 'WasteCodes'] || '').trim();
    var wq = (manifest['waste' + wlc + 'Qty'] || '').trim();
    if (wd || wco || wq) wasteLineCount = wlc;
  }
  if (wasteLineCount < 4) wasteLineCount = 4;
  var totalPages = wasteLineCount <= 4 ? 1 : Math.ceil((wasteLineCount - 4) / CONT_MAX_WASTE_LINES) + 1;

  // === Page 1 - Main Form (8700-22) ===
  placeAt(M.generatorEpaId.row, M.generatorEpaId.col, manifest.generatorEpaId);
  if (manifest.pageNum) placeAt(M.page.row, M.page.col, manifest.pageNum);
  if (manifest.pageTotal) placeAt(M.totalPages.row, M.totalPages.col, manifest.pageTotal);
  placeAt(M.emergencyPhone.row, M.emergencyPhone.col, manifest.emergencyPhone);
  placeAt(M.generatorName.row, M.generatorName.col, manifest.generatorName);
  placeAt(M.generatorPhone.row, M.generatorPhone.col, manifest.generatorPhone);
  placeAt(M.generatorMailAddr.row, M.generatorMailAddr.col, manifest.generatorAddress);
  placeAt(M.generatorMailCity.row, M.generatorMailCity.col, manifest.generatorCityStZip);
  placeAt(M.generatorSiteAddr.row, M.generatorSiteAddr.col, manifest.genSiteAddress);
  placeAt(M.generatorSiteCity.row, M.generatorSiteCity.col, manifest.genSiteCityStZip);
  placeAt(M.transporter1Name.row, M.transporter1Name.col, manifest.transporter1Name);
  placeAt(M.transporter1EpaId.row, M.transporter1EpaId.col, manifest.transporter1EpaId);
  placeAt(M.transporter2Name.row, M.transporter2Name.col, manifest.transporter2Name);
  placeAt(M.transporter2EpaId.row, M.transporter2EpaId.col, manifest.transporter2EpaId);
  placeAt(M.facilityName.row, M.facilityName.col, manifest.facilityName);
  placeAt(M.facilityEpaId.row, M.facilityEpaId.col, manifest.facilityEpaId);
  placeAt(M.facilityAddress.row, M.facilityAddress.col, manifest.facilityAddress);
  placeAt(M.facilityPhone.row, M.facilityPhone.col, manifest.facilityPhone);
  placeAt(M.facilityCity.row, M.facilityCity.col, manifest.facilityCityStZip);

  // DOT ERG (Emergency Response Guidebook) lookup by UN/NA number
  var ERG_LOOKUP = {
    '1005': '125', '1017': '124', '1049': '115', '1072': '122', '1075': '115',
    '1090': '127', '1170': '127', '1202': '128', '1203': '128', '1219': '129',
    '1230': '131', '1263': '128', '1268': '128', '1270': '128', '1381': '136',
    '1402': '138', '1547': '153', '1593': '160', '1760': '154', '1789': '157',
    '1805': '154', '1824': '154', '1830': '137', '1831': '137', '1863': '128',
    '1950': '126', '1978': '115', '1992': '131', '1993': '128', '1999': '130',
    '2014': '140', '2031': '157', '2078': '156', '2312': '153', '2672': '154',
    '2794': '154', '2795': '154', '2809': '172', '2810': '153', '2920': '132',
    '2924': '132', '3077': '171', '3082': '171', '3175': '133', '3257': '171',
    '3258': '171', '3264': '154', '3266': '154', '3291': '158', '3334': '171',
    '3335': '171'
  };

  // Extract UN/NA number from description and look up ERG guide
  function getErgNumber(text) {
    if (!text) return '';
    var match = text.match(/(?:UN|NA)\s*(\d{4})/i);
    if (match && ERG_LOOKUP[match[1]]) {
      return ERG_LOOKUP[match[1]];
    }
    return '';
  }

  // Format shipping description: Title Case with exceptions
  // n.o.s. = lowercase, PG/UN/NA/RQ = uppercase, everything else = Title Case
  function formatShipDesc(text) {
    if (!text) return '';
    // First lowercase everything, then title-case each word
    var result = text.toLowerCase().replace(/\b\w/g, function(c) { return c.toUpperCase(); });
    // Fix exceptions: n.o.s. should be all lowercase
    result = result.replace(/N\.O\.S\./gi, 'n.o.s.');
    result = result.replace(/\bNos\b/gi, 'n.o.s.');
    // PG should be uppercase
    result = result.replace(/\bPg\b/g, 'PG');
    // UN and NA (hazmat ID prefixes) should be uppercase
    result = result.replace(/\bUn(\d)/g, 'UN$1');
    result = result.replace(/\bNa(\d)/g, 'NA$1');
    // RQ should be uppercase
    result = result.replace(/\bRq,/g, 'RQ,');
    result = result.replace(/\bRq\b/g, 'RQ');
    return result;
  }

  // Waste Lines 1-4
  var maxOnPage1 = Math.min(wasteLineCount, 4);
  for (var w = 1; w <= maxOnPage1; w++) {
    var hmKey = 'waste' + w + 'hm';
    var descKey = 'waste' + w + 'desc';
    var baseRow = M[hmKey].row;
    placeAt(M[hmKey].row, M[hmKey].col, manifest['waste' + w + 'HM']);
    var rawDesc = manifest['waste' + w + 'Description'] || '';
    var ergNum = getErgNumber(rawDesc);
    var descText = formatShipDesc(rawDesc);
    if (ergNum && descText.indexOf('ERG') === -1) descText += ', ERG # ' + ergNum;
    var descMaxFirst = M['waste' + w + 'containerNum'].col - M[descKey].col - 1;
    var descLines = wrapDesc(descText, descMaxFirst, 55);
    for (var dl = 0; dl < descLines.length && dl < 2; dl++) {
      placeAt(baseRow + dl, M[descKey].col, descLines[dl]);
    }
    placeAt(baseRow, M['waste' + w + 'containerNum'].col, manifest['waste' + w + 'ContainerNum']);
    placeAt(baseRow, M['waste' + w + 'container'].col, manifest['waste' + w + 'ContainerType']);
    placeAt(baseRow, M['waste' + w + 'qty'].col, manifest['waste' + w + 'Qty']);
    placeAt(baseRow, M['waste' + w + 'uom'].col, manifest['waste' + w + 'Unit']);
    var wcKey = 'waste' + w + 'wc';
    var codes = parseWC((manifest['waste' + w + 'WasteCodes'] || '').trim());
    for (var ci = 0; ci < 6 && ci < codes.length; ci++) {
      var wcField = wcKey + (ci + 1);
      if (M[wcField]) {
        placeAt(M[wcField].row, M[wcField].col, codes[ci]);
      }
    }
  }

  // Box 14 - Special Handling
  var sh1 = manifest.specialHandling || '';
  var sh2 = manifest.specialHandling2 || '';
  var sh3 = manifest.specialHandling3 || '';
  if (!sh1 && !sh2) {
    var parts14 = [];
    for (var b14 = 1; b14 <= wasteLineCount; b14++) {
      var pid14 = manifest['waste' + b14 + 'ProfileId'] || '';
      var csize14 = manifest['waste' + b14 + 'ContainerSize'] || '';
      var ctype14 = manifest['waste' + b14 + 'ContainerType'] || '';
      var desc14 = manifest['waste' + b14 + 'Description'] || '';
      if (!desc14 && !pid14) continue;
      var label14 = '9b.' + b14 + '= ';
      if (pid14) label14 += pid14;
      if (csize14) label14 += ' ' + csize14;
      if (ctype14) label14 += ' ' + ctype14;
      parts14.push(label14.trim());
    }
    var autoText = parts14.join(', ');
    if (autoText.length > 75) {
      sh1 = autoText.substring(0, 75);
      var rest14 = autoText.substring(75);
      sh2 = rest14.length > 75 ? rest14.substring(0, 75) : rest14;
    } else {
      sh1 = autoText;
    }
  }
  placeAt(M.specialHandling.row, M.specialHandling.col, sh1);
  placeAt(M.specialHandling2.row, M.specialHandling2.col, sh2);
  placeAt(M.specialHandling3.row, M.specialHandling3.col, sh3);
  placeAt(M.generatorCertName.row, M.generatorCertName.col, manifest.generatorPrintName);

  // === Continuation Pages ===
  if (wasteLineCount > 4) {
    var contMap = FORM_8700_22A_MAP;
    var remainingLines = wasteLineCount - 4;
    var contPageNum = 2;
    var manifestLineStart = 5;
    var contPageCount = Math.ceil(remainingLines / CONT_MAX_WASTE_LINES);
    for (var cpIdx = 0; cpIdx < contPageCount; cpIdx++) {
      var pg = cpIdx + 2;
      var linesOnThisPage = Math.min(remainingLines, CONT_MAX_WASTE_LINES);
      placeAt(contMap.generatorEpaId.row, contMap.generatorEpaId.col, manifest.generatorEpaId, pg);
      placeAt(contMap.page.row, contMap.page.col, manifest.contPageNum || String(contPageNum), pg);
      placeAt(contMap.totalPages.row, contMap.totalPages.col, String(totalPages), pg);
      placeAt(contMap.manifestTrackingNum.row, contMap.manifestTrackingNum.col, manifest.manifestTrackingNum, pg);
      placeAt(contMap.generatorName.row, contMap.generatorName.col, manifest.generatorName, pg);
      placeAt(contMap.contTransporterName.row, contMap.contTransporterName.col, manifest.contTransporterName || manifest.transporter1Name, pg);
      placeAt(contMap.contTransporterEpaId.row, contMap.contTransporterEpaId.col, manifest.contTransporterEpaId || manifest.transporter1EpaId, pg);
      placeAt(contMap.contTransporter2Name.row, contMap.contTransporter2Name.col, manifest.contTransporter2Name, pg);
      placeAt(contMap.contTransporter2EpaId.row, contMap.contTransporter2EpaId.col, manifest.contTransporter2EpaId, pg);
      for (var cw = 0; cw < linesOnThisPage; cw++) {
        var mLineNum = manifestLineStart + cw;
        var contRow = CONT_WASTE_START_ROW + (cw * CONT_WASTE_ROW_SPACING);
        var cwRawDesc = manifest['waste' + mLineNum + 'Description'] || '';
        var cwErgNum = getErgNumber(cwRawDesc);
        var cwDesc = formatShipDesc(cwRawDesc);
        if (cwErgNum && cwDesc.indexOf('ERG') === -1) cwDesc += ', ERG # ' + cwErgNum;
        var cwDescLines = wrapDesc(cwDesc, 45, 55);
        for (var cdl = 0; cdl < cwDescLines.length && cdl < 2; cdl++) {
          placeAt(contRow + cdl, M.waste1desc.col, cwDescLines[cdl], pg);
        }
        placeAt(contRow, M.waste1hm.col, manifest['waste' + mLineNum + 'HM'], pg);
        placeAt(contRow, M.waste1containerNum.col, manifest['waste' + mLineNum + 'ContainerNum'], pg);
        placeAt(contRow, M.waste1container.col, manifest['waste' + mLineNum + 'ContainerType'], pg);
        placeAt(contRow, M.waste1qty.col, manifest['waste' + mLineNum + 'Qty'], pg);
        placeAt(contRow, M.waste1uom.col, manifest['waste' + mLineNum + 'Unit'], pg);
        var cwCodes = parseWC((manifest['waste' + mLineNum + 'WasteCodes'] || '').trim());
        for (var cci = 0; cci < 6 && cci < cwCodes.length; cci++) {
          var cwRow2 = contRow + (cci >= 3 ? 1 : 0);
          var cwColOff = (cci % 3) * 5;
          placeAt(cwRow2, M.waste1wc1.col + cwColOff, cwCodes[cci], pg);
        }
      }
      var cSh1 = manifest.contSpecialHandling || sh1;
      var cSh2 = manifest.contSpecialHandling2 || sh2;
      var cSh3 = manifest.contSpecialHandling3 || sh3;
      placeAt(contMap.specialHandling.row, contMap.specialHandling.col, cSh1, pg);
      placeAt(contMap.specialHandling2.row, contMap.specialHandling2.col, cSh2, pg);
      placeAt(contMap.specialHandling3.row, contMap.specialHandling3.col, cSh3, pg);
      placeAt(contMap.contTransporterPrintName.row, contMap.contTransporterPrintName.col, manifest.contTransporterPrintName, pg);
      placeAt(contMap.contTransporterDate.row, contMap.contTransporterDate.col, manifest.contTransporterDate, pg);
      placeAt(contMap.contTransporter2PrintName.row, contMap.contTransporter2PrintName.col, manifest.contTransporter2PrintName, pg);
      placeAt(contMap.contTransporter2Date.row, contMap.contTransporter2Date.col, manifest.contTransporter2Date, pg);
      placeAt(contMap.contDiscrepancyInfo.row, contMap.contDiscrepancyInfo.col, manifest.contDiscrepancyInfo, pg);
      remainingLines -= linesOnThisPage;
      manifestLineStart += linesOnThisPage;
      contPageNum++;
    }
  }

  // === Build HTML with CSS absolute positioning ===
  // Printer: Epson LQ-590II at 15 CPI, 6 LPI
  // Column positions in RAW_MAP are on a 12-col-per-inch grid (form layout).
  // Font at 8pt Courier New ≈ 15 CPI to match printer character width.
  // Base offsets account for pre-printed form header area.
  // Fine-tune via query params: ?rowOffset=0.1&colOffset=-0.1
  var CPI = 12;  // column grid (form layout units, NOT printer CPI)
  var LPI = 6;   // lines per inch
  var BASE_TOP_OFFSET = 0.5;   // inches - shift down for form header (v45 value)
  var BASE_LEFT_OFFSET = 0.0;  // inches - left adjustment
  var colOffsetIn = BASE_LEFT_OFFSET + (parseFloat(req.query.colOffset) || 0);
  var rowOffsetIn = BASE_TOP_OFFSET + (parseFloat(req.query.rowOffset) || 0);

  var html = '<!DOCTYPE html><html><head><title>Print Manifest</title><style>';
  html += '@page { margin: 0; size: 8.5in 11in; }';
  html += '@media print { body { margin: 0; padding: 0; } .no-print { display: none !important; } }';
  html += 'body { margin: 0; padding: 0; }';
  html += '.page { position: relative; width: 8.5in; height: 11in; overflow: hidden; page-break-after: always; }';
  html += '.page:last-child { page-break-after: auto; }';
  html += '.field { position: absolute; font-family: "Courier New", Courier, monospace; font-size: 10pt; line-height: 1; white-space: pre; margin: 0; padding: 0; }';
  html += '.toolbar { padding: 10px; background: #f0f0f0; text-align: center; font-family: sans-serif; }';
  html += '.toolbar button { padding: 8px 20px; font-size: 16px; margin: 0 5px; cursor: pointer; }';
  html += '.toolbar .print-btn { background: #7c3aed; color: white; border: none; border-radius: 4px; }';
  html += '.toolbar .close-btn { background: #6b7280; color: white; border: none; border-radius: 4px; }';
  html += '.toolbar label { margin: 0 8px; font-size: 13px; }';
  html += '.toolbar input[type=number] { width: 60px; padding: 2px 4px; }';
  html += '</style></head><body>';

  // Toolbar (hidden when printing)
  html += '<div class="no-print toolbar">';
  html += '<button class="print-btn" onclick="window.print()">Print Manifest</button>';
  html += '<button class="close-btn" onclick="window.close()">Close</button>';
  html += '<span style="margin-left:20px;font-size:12px;color:#666">Select your Epson LQ-590II in the print dialog. Set margins to None.</span>';
  html += '</div>';

  // Group placements by page
  var maxPage = 1;
  for (var pi = 0; pi < placements.length; pi++) {
    if (placements[pi].page > maxPage) maxPage = placements[pi].page;
  }

  for (var pg = 1; pg <= maxPage; pg++) {
    html += '<div class="page">';
    for (var fi = 0; fi < placements.length; fi++) {
      var p = placements[fi];
      if (p.page !== pg) continue;
      // Convert row/col to inches: col 1 = 0in from left, row 1 = 0in from top
      var leftIn = ((p.col - 1) / CPI) + colOffsetIn;
      var topIn = ((p.row - 1) / LPI) + rowOffsetIn;
      // Escape HTML
      var safeText = p.text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      html += '<span class="field" style="left:' + leftIn.toFixed(4) + 'in;top:' + topIn.toFixed(4) + 'in;">' + safeText + '</span>';
    }
    html += '</div>';
  }

  html += '</body></html>';
  res.type('html').send(html);
});

// Serve static files
app.use(express.static(path.join(__dirname)));

// Start server
app.listen(PORT, function() {
  console.log('Manifest Platform running on port ' + PORT);
  console.log('Data directory: ' + DATA_DIR);
});

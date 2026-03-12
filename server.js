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
// Ensure labels array exists for older data files
if (!data.labels) { data.labels = []; saveData(data); }

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

// Helper: mark a manifest as printed/filed under its generator
function fileManifestAsPrinted(manifestId) {
  var manifests = data.manifests || [];
  for (var i = 0; i < manifests.length; i++) {
    if (manifests[i].id === manifestId) {
      if (!manifests[i].printHistory) manifests[i].printHistory = [];
      manifests[i].printHistory.push({ printedAt: new Date().toISOString() });
      manifests[i].lastPrintedAt = new Date().toISOString();
      manifests[i].status = 'printed';
      saveData(data);
      broadcast('update', { collection: 'manifests', action: 'update', item: manifests[i] });
      return manifests[i];
    }
  }
  return null;
}

// Helper: mark a BOL as printed/filed
function fileBolAsPrinted(bolId) {
  var bols = data.bols || [];
  for (var i = 0; i < bols.length; i++) {
    if (bols[i].id === bolId) {
      if (!bols[i].printHistory) bols[i].printHistory = [];
      bols[i].printHistory.push({ printedAt: new Date().toISOString() });
      bols[i].lastPrintedAt = new Date().toISOString();
      bols[i].status = 'printed';
      saveData(data);
      return bols[i];
    }
  }
  return null;
}

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

// One-time backfill: link existing imported profiles to generators
// Uses the profiles table (which has generatorName) to find matching waste streams and generators
(function backfillProfileLinks() {
  var profiles = data.profiles || [];
  var generators = data.generators || [];
  var wasteStreams = data.wasteStreams || [];
  var changed = false;
  for (var pi = 0; pi < profiles.length; pi++) {
    var prof = profiles[pi];
    if (!prof.generatorName) continue;
    var profGenLower = prof.generatorName.toLowerCase().trim();
    // Find the matching generator by name (case-insensitive)
    var matchedGen = null;
    for (var gi = 0; gi < generators.length; gi++) {
      if (generators[gi].name && generators[gi].name.toLowerCase().trim() === profGenLower) {
        matchedGen = generators[gi]; break;
      }
    }
    if (!matchedGen) continue;
    // Find the waste stream by name matching the profile's wasteStreamName
    var matchedWs = null;
    for (var wi = 0; wi < wasteStreams.length; wi++) {
      if (wasteStreams[wi].name && wasteStreams[wi].name === prof.wasteStreamName) {
        matchedWs = wasteStreams[wi]; break;
      }
    }
    if (!matchedWs) continue;
    // Also backfill generatorName on the waste stream if missing
    if (!matchedWs.generatorName && prof.generatorName) {
      matchedWs.generatorName = prof.generatorName;
      changed = true;
    }
    // Link waste stream to generator's profileIds
    if (!matchedGen.profileIds) matchedGen.profileIds = [];
    if (matchedGen.profileIds.indexOf(matchedWs.id) < 0) {
      matchedGen.profileIds.push(matchedWs.id);
      changed = true;
      console.log('Backfill: linked "' + matchedWs.name + '" to generator "' + matchedGen.name + '"');
    }
  }
  if (changed) saveData(data);
})();

// Manual re-link endpoint - triggers the backfill on demand
app.post('/api/relink-profiles', function(req, res) {
  var profiles = data.profiles || [];
  var generators = data.generators || [];
  var wasteStreams = data.wasteStreams || [];
  var linked = 0;
  for (var pi = 0; pi < profiles.length; pi++) {
    var prof = profiles[pi];
    if (!prof.generatorName) continue;
    var profGenLower = prof.generatorName.toLowerCase().trim();
    var matchedGen = null;
    for (var gi = 0; gi < generators.length; gi++) {
      if (generators[gi].name && generators[gi].name.toLowerCase().trim() === profGenLower) {
        matchedGen = generators[gi]; break;
      }
    }
    if (!matchedGen) continue;
    var matchedWs = null;
    for (var wi = 0; wi < wasteStreams.length; wi++) {
      if (wasteStreams[wi].name && wasteStreams[wi].name === prof.wasteStreamName) {
        matchedWs = wasteStreams[wi]; break;
      }
    }
    if (!matchedWs) continue;
    if (!matchedWs.generatorName && prof.generatorName) {
      matchedWs.generatorName = prof.generatorName;
    }
    if (!matchedGen.profileIds) matchedGen.profileIds = [];
    if (matchedGen.profileIds.indexOf(matchedWs.id) < 0) {
      matchedGen.profileIds.push(matchedWs.id);
      linked++;
    }
  }
  if (linked > 0) saveData(data);
  res.json({ success: true, linked: linked });
});

// Auto-link a waste stream to its matching generator by EPA ID or name (case-insensitive)
function autoLinkWasteStreamToGenerator(ws) {
  if (!ws || !data.generators) return;
  var matchedGen = null;
  // First try EPA ID match (most reliable)
  if (ws.generatorEpaId) {
    for (var i = 0; i < data.generators.length; i++) {
      if (data.generators[i].epaId && data.generators[i].epaId.toUpperCase() === ws.generatorEpaId.toUpperCase()) {
        matchedGen = data.generators[i]; break;
      }
    }
  }
  // Fallback: match by generator name (case-insensitive)
  if (!matchedGen && ws.generatorName) {
    var wsGenLower = ws.generatorName.toLowerCase().trim();
    for (var j = 0; j < data.generators.length; j++) {
      if (data.generators[j].name && data.generators[j].name.toLowerCase().trim() === wsGenLower) {
        matchedGen = data.generators[j]; break;
      }
    }
  }
  if (matchedGen) {
    if (!matchedGen.profileIds) matchedGen.profileIds = [];
    if (matchedGen.profileIds.indexOf(ws.id) < 0) {
      matchedGen.profileIds.push(ws.id);
      console.log('Auto-linked waste stream "' + ws.name + '" to generator "' + matchedGen.name + '"');
    }
  }
}

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

        // Auto-link waste stream to matching generator by EPA ID or name
        autoLinkWasteStreamToGenerator(ewsWasteStream);

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

        // Auto-link waste stream to matching generator by EPA ID or name
        autoLinkWasteStreamToGenerator(smxWasteStream);

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
      // Try "PROFILE #: 070128303- 13048" format (Republic) - capture full compound ID
      var pidMatchFull = fullText.match(/PROFILE\s*#[:\s]+([\d]+[\s\-]*[\d]*)/i);
      if (pidMatchFull) {
        profileId = pidMatchFull[1].replace(/[\s\-]+/g, '-').replace(/-$/, '');
      }
      // Fallback: try "Profile ID: 1234567"
      if (!profileId) {
        var pidMatch = fullText.match(/Profile\s*ID[:\s]+(\d[\d\s\-]*\d)/i);
        if (pidMatch) profileId = pidMatch[1].replace(/[\s]+/g, '').trim();
      }
      // Fallback: try "Profile #" or "Profile No" or "Profile Number" patterns
      if (!profileId) {
        var pidMatch2 = fullText.match(/Profile\s*(?:#|No\.?|Number)[:\s]*([\d][\d\s\-]*[\d])/i);
        if (pidMatch2) profileId = pidMatch2[1].replace(/[\s]+/g, '').trim();
      }
      // Fallback: try "Approval #" or "Approval Number" (some Republic profiles use this)
      if (!profileId) {
        var pidMatch3 = fullText.match(/Approval\s*(?:#|No\.?|Number)[:\s]*([\d][\d\s\-]*[\d])/i);
        if (pidMatch3) profileId = pidMatch3[1].replace(/[\s]+/g, '').trim();
      }

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

      // Extract generator EPA ID from Section A
      var repGenEpaId = '';
      var repEpaMatch = fullText.match(/EPA\s*(?:ID|Identification)\s*(?:#|No\.?|Number)?[:\s]+([A-Z]{2}[A-Z0-9]{8,12})/i);
      if (repEpaMatch) repGenEpaId = repEpaMatch[1].trim();
      if (!repGenEpaId) {
        // Look for standalone EPA ID pattern near generator section
        var genSection = fullText.match(/(?:Generator|Section\s*A)[\s\S]{0,500}/i);
        if (genSection) {
          var epaInGen = genSection[0].match(/\b([A-Z]{2}[A-Z0-9]{8,12})\b/);
          if (epaInGen) repGenEpaId = epaInGen[1].trim();
        }
      }

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
        generatorName: isAddGen ? '' : repGenName,
        generatorEpaId: repGenEpaId,
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

      // Auto-link waste stream to matching generator by EPA ID or name
      autoLinkWasteStreamToGenerator(wasteStream);

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
  // Box 4 - Manifest Tracking Number
  manifestTrackingNum: { row: 2, col: 55 },
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
  // Box 27-31 - Waste line columns (same col positions as RAW_22A_MAP for consistency)
  wasteHm:                  { col: 3, rowOffset: 0 },
  wasteDesc:                { col: 7, rowOffset: 0 },
  wasteContainerNum:        { col: 54, rowOffset: 0 },
  wasteContainerType:       { col: 59, rowOffset: 0 },
  wasteQty:                 { col: 64, rowOffset: 0 },
  wasteUom:                 { col: 70, rowOffset: 0 },
  wasteWc1:                 { col: 76, rowOffset: 0 },
  wasteWc2:                 { col: 81, rowOffset: 0 },
  wasteWc3:                 { col: 86, rowOffset: 0 },
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
var CONT_WASTE_START_ROW = 17;  // Box 27 waste data rows on 8700-22A (after header)
var CONT_WASTE_ROW_SPACING = 3;
var CONT_MAX_WASTE_LINES = 8;

// RAW 22A MAP for Epson direct print (calibrated separately from browser print)
var RAW_22A_MAP = {
  // Box 21 - Generator's US EPA ID Number
  generatorEpaId:           { row: 6, col: 26 },
  // Box 22 - Page __ of __
  page:                     { row: 6, col: 47 },
  totalPages:               { row: 6, col: 50 },
  // Box 23 - Manifest Tracking Number
  manifestTrackingNum:      { row: 6, col: 62 },
  // Box 24 - Generator's Name & EPA ID
  generatorName:            { row: 9, col: 8 },
  generatorEpaId2:          { row: 9, col: 62 },
  // Box 25 - Transporter Company Name & EPA ID
  contTransporterName:      { row: 11, col: 8 },
  contTransporterEpaId:     { row: 11, col: 62 },
  // Box 26 - Transporter 2 Company Name & EPA ID
  contTransporter2Name:     { row: 13, col: 8 },
  contTransporter2EpaId:    { row: 13, col: 62 },
  // Box 27 - Waste line columns (8700-22A layout - each field positioned independently)
  wasteHm:                  { col: 3, rowOffset: 0 },
  wasteDesc:                { col: 7, rowOffset: 0 },
  wasteContainerNum:        { col: 54, rowOffset: 0 },
  wasteContainerType:       { col: 59, rowOffset: 0 },
  wasteQty:                 { col: 64, rowOffset: 0 },
  wasteUom:                 { col: 70, rowOffset: 0 },
  wasteWc1:                 { col: 76, rowOffset: 0 },
  wasteWc2:                 { col: 81, rowOffset: 0 },
  wasteWc3:                 { col: 86, rowOffset: 0 },
  // Box 32 - Special Handling Instructions
  specialHandling:          { row: 47, col: 8 },
  specialHandling2:         { row: 48, col: 8 },
  specialHandling3:         { row: 49, col: 8 },
  // Box 33 - Transporter Acknowledgment of Receipt
  contTransporterPrintName: { row: 51, col: 8 },
  contTransporterDate:      { row: 51, col: 62 },
  // Box 34 - Transporter 2 Acknowledgment
  contTransporter2PrintName:{ row: 53, col: 8 },
  contTransporter2Date:     { row: 53, col: 62 },
  // Box 35 - Discrepancy
  contDiscrepancyInfo:      { row: 55, col: 8 }
};

// Print manifest - plain text for dot matrix
// Epson LQ-590II at 12 CPI, tractor feed locked all the way left
// Pinfeed manifests with strips on left and right sides (~0.5" each = ~6 chars at 12 CPI)
// MAP column values already account for the left pinfeed strip offset
var BUILD_VERSION = 'v76-2026-03-10';
app.get('/api/version', function(req, res) { res.json({ version: BUILD_VERSION }); });

// Debug: show profile linking for a generator by name
app.get('/api/debug/generator-profiles/:name', function(req, res) {
  var searchName = decodeURIComponent(req.params.name).toLowerCase().trim();
  // Find generator
  var gen = null;
  for (var i = 0; i < (data.generators || []).length; i++) {
    if ((data.generators[i].name || '').toLowerCase().trim() === searchName) { gen = data.generators[i]; break; }
  }
  // Find profiles in profiles table for this generator name
  var matchingProfiles = (data.profiles || []).filter(function(p) {
    return p.generatorName && p.generatorName.toLowerCase().trim() === searchName;
  });
  // Find waste streams
  var allWs = data.wasteStreams || [];
  var profLinkedNames = {};
  matchingProfiles.forEach(function(p) { if (p.wasteStreamName) profLinkedNames[p.wasteStreamName] = true; });
  var wsMatches = allWs.map(function(w) {
    var reasons = [];
    if (gen && (gen.profileIds || []).indexOf(w.id) >= 0) reasons.push('profileIds');
    if (gen && gen.epaId && w.generatorEpaId && w.generatorEpaId.toUpperCase() === gen.epaId.toUpperCase()) reasons.push('epaId');
    if (w.generatorName && w.generatorName.toLowerCase().trim() === searchName) reasons.push('generatorName');
    if (profLinkedNames[w.name]) reasons.push('profilesTable');
    return reasons.length > 0 ? { wsId: w.id, wsName: w.name, wsProfileId: w.profileId, wsGeneratorName: w.generatorName || '', wsGeneratorEpaId: w.generatorEpaId || '', matchedBy: reasons } : null;
  }).filter(Boolean);
  res.json({
    searchName: searchName,
    generator: gen ? { id: gen.id, name: gen.name, epaId: gen.epaId, profileIds: gen.profileIds || [] } : null,
    profileRecords: matchingProfiles.map(function(p) { return { profileId: p.profileId, wasteStreamName: p.wasteStreamName, generatorName: p.generatorName }; }),
    matchedWasteStreams: wsMatches,
    totalWasteStreams: allWs.length,
    totalProfiles: (data.profiles || []).length
  });
});

// Debug endpoint - inspect manifest waste line data
app.get('/api/debug/manifest/:id', function(req, res) {
  var manifest = null;
  for (var i = 0; i < (data.manifests || []).length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).json({ error: 'Not found' });
  var debug = { id: manifest.id, wasteLineCount: manifest.wasteLineCount };
  for (var w = 1; w <= 10; w++) {
    var desc = manifest['waste' + w + 'Description'] || '';
    var pid = manifest['waste' + w + 'ProfileId'] || '';
    var csize = manifest['waste' + w + 'ContainerSize'] || '';
    var ctype = manifest['waste' + w + 'ContainerType'] || '';
    if (desc || pid) {
      debug['waste' + w] = { desc: desc, profileId: pid, containerSize: csize, containerType: ctype };
    }
  }
  debug.specialHandling = manifest.specialHandling || '';
  debug.specialHandling2 = manifest.specialHandling2 || '';
  debug.specialHandling3 = manifest.specialHandling3 || '';
  debug.alignment = { colShift: colShift, rowShift: rowShift, customAlignment: customAlignment ? 'yes' : 'no' };
  res.json(debug);
});

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

// Saved defaults: when user clicks "Set Current as Default", current positions are stored here
// These override the hardcoded MAPs as the baseline
var savedDefaults = data.savedDefaults || null;
var savedDefaults22a = data.savedDefaults22a || null;

function getBaseMap() {
  return savedDefaults || FORM_8700_MAP;
}

function getBaseRawMap() {
  // For RAW_MAP, we compute from saved defaults using delta approach (same as getActiveRawMap)
  if (!savedDefaults) return RAW_MAP;
  var merged = {};
  var keys = Object.keys(RAW_MAP);
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    if (savedDefaults[key] && FORM_8700_MAP[key]) {
      var deltaRow = savedDefaults[key].row - FORM_8700_MAP[key].row;
      var deltaCol = savedDefaults[key].col - FORM_8700_MAP[key].col;
      merged[key] = { row: RAW_MAP[key].row + deltaRow, col: RAW_MAP[key].col + deltaCol };
    } else {
      merged[key] = RAW_MAP[key];
    }
  }
  return merged;
}

function getBaseRaw22aMap() {
  return savedDefaults22a || RAW_22A_MAP;
}

function getBaseForm22aMap() {
  return savedDefaults22a || FORM_8700_22A_MAP;
}

function getActiveMap() {
  var base = getBaseMap();
  if (!customAlignment) return base;
  var merged = {};
  var keys = Object.keys(base);
  for (var i = 0; i < keys.length; i++) {
    if (customAlignment[keys[i]]) {
      merged[keys[i]] = customAlignment[keys[i]];
    } else {
      merged[keys[i]] = base[keys[i]];
    }
  }
  return merged;
}

app.get('/api/alignment', function(req, res) {
  // Re-sync in-memory vars from data object to ensure they're current after saves
  colShift = (typeof data.colShift === 'number') ? data.colShift : 0;
  rowShift = (typeof data.rowShift === 'number') ? data.rowShift : 0;
  customAlignment = data.customAlignment || null;
  previousAlignment = data.previousAlignment || null;
  savedDefaults = data.savedDefaults || null;
  console.log('Alignment GET: colShift=' + colShift + ', rowShift=' + rowShift + ', customAlignment=' + (customAlignment ? 'yes(' + Object.keys(customAlignment).length + ' keys)' : 'null') + ', savedDefaults=' + (savedDefaults ? 'yes' : 'no'));
  res.json({
    fields: getActiveMap(),
    map: getActiveMap(),
    defaults: getBaseMap(),
    hasPrevious: previousAlignment !== null,
    hasSavedDefaults: savedDefaults !== null,
    colShift: colShift,
    rowShift: rowShift
  });
});

app.put('/api/alignment', function(req, res) {
  console.log('Alignment SAVE: colShift=' + req.body.colShift + ', rowShift=' + req.body.rowShift + ', fieldsKeys=' + (req.body.fields ? Object.keys(req.body.fields).length : 'null'));
  previousAlignment = customAlignment ? JSON.parse(JSON.stringify(customAlignment)) : null;
  data.previousAlignment = previousAlignment;
  customAlignment = req.body.fields || req.body.map || null;
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
  console.log('Alignment SAVED: colShift=' + data.colShift + ', rowShift=' + data.rowShift + ', customAlignment=' + (data.customAlignment ? 'yes' : 'no'));
  // Verify by reading back from disk and re-sync in-memory vars
  try {
    var verify = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    console.log('Alignment VERIFY from disk: colShift=' + verify.colShift + ', rowShift=' + verify.rowShift);
    // Re-sync in-memory from what's actually on disk
    colShift = (typeof verify.colShift === 'number') ? verify.colShift : 0;
    rowShift = (typeof verify.rowShift === 'number') ? verify.rowShift : 0;
    customAlignment = verify.customAlignment || null;
    previousAlignment = verify.previousAlignment || null;
  } catch (e) {
    console.error('Alignment VERIFY FAILED:', e.message);
  }
  res.json({ ok: true, colShift: colShift, rowShift: rowShift });
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
  res.json({ ok: true, fields: getActiveMap(), map: getActiveMap(), colShift: colShift, rowShift: rowShift });
});

// Bake current alignment as new defaults (main form)
app.post('/api/alignment/bake-defaults', function(req, res) {
  var current = getActiveMap();
  savedDefaults = JSON.parse(JSON.stringify(current));
  data.savedDefaults = savedDefaults;
  // Clear custom overrides since they're now baked into defaults
  previousAlignment = customAlignment ? JSON.parse(JSON.stringify(customAlignment)) : null;
  data.previousAlignment = previousAlignment;
  customAlignment = null;
  delete data.customAlignment;
  colShift = 0;
  data.colShift = 0;
  rowShift = 0;
  data.rowShift = 0;
  saveData(data);
  console.log('Alignment BAKED: savedDefaults now has ' + Object.keys(savedDefaults).length + ' fields');
  res.json({ ok: true, message: 'Current settings saved as new defaults' });
});

// 22A (Continuation Page) Alignment System
var customAlignment22a = data.customAlignment22a || null;
var previousAlignment22a = data.previousAlignment22a || null;

function getActiveRaw22aMap() {
  var base = getBaseRaw22aMap();
  if (!customAlignment22a) return base;
  var merged = {};
  var keys = Object.keys(base);
  for (var i = 0; i < keys.length; i++) {
    if (customAlignment22a[keys[i]]) {
      merged[keys[i]] = customAlignment22a[keys[i]];
    } else {
      merged[keys[i]] = base[keys[i]];
    }
  }
  return merged;
}

function getActiveForm22aMap() {
  var base = getBaseForm22aMap();
  if (!customAlignment22a) return base;
  var merged = {};
  var keys = Object.keys(base);
  for (var i = 0; i < keys.length; i++) {
    if (customAlignment22a[keys[i]]) {
      merged[keys[i]] = customAlignment22a[keys[i]];
    } else {
      merged[keys[i]] = base[keys[i]];
    }
  }
  return merged;
}

app.get('/api/alignment22a', function(req, res) {
  // Re-sync in-memory vars from data object
  customAlignment22a = data.customAlignment22a || null;
  previousAlignment22a = data.previousAlignment22a || null;
  savedDefaults22a = data.savedDefaults22a || null;
  console.log('Alignment22a GET: customAlignment22a=' + (customAlignment22a ? 'yes(' + Object.keys(customAlignment22a).length + ' keys)' : 'null') + ', savedDefaults22a=' + (savedDefaults22a ? 'yes' : 'no'));
  res.json({
    fields: getActiveRaw22aMap(),
    map: getActiveRaw22aMap(),
    defaults: getBaseRaw22aMap(),
    colShift: (typeof data.colShift22a === 'number') ? data.colShift22a : 0,
    rowShift: (typeof data.rowShift22a === 'number') ? data.rowShift22a : 0,
    hasPrevious: previousAlignment22a !== null,
    hasSavedDefaults: savedDefaults22a !== null
  });
});

app.put('/api/alignment22a', function(req, res) {
  console.log('Alignment22a SAVE: fieldsKeys=' + (req.body.fields ? Object.keys(req.body.fields).length : 'null'));
  previousAlignment22a = customAlignment22a ? JSON.parse(JSON.stringify(customAlignment22a)) : null;
  data.previousAlignment22a = previousAlignment22a;
  data.previousColShift22a = (typeof data.colShift22a === 'number') ? data.colShift22a : 0;
  data.previousRowShift22a = (typeof data.rowShift22a === 'number') ? data.rowShift22a : 0;
  customAlignment22a = req.body.fields || req.body.map || null;
  data.customAlignment22a = customAlignment22a;
  if (typeof req.body.colShift === 'number') data.colShift22a = req.body.colShift;
  if (typeof req.body.rowShift === 'number') data.rowShift22a = req.body.rowShift;
  saveData(data);
  console.log('Alignment22a SAVED: customAlignment22a=' + (data.customAlignment22a ? 'yes' : 'no') + ', colShift=' + data.colShift22a + ', rowShift=' + data.rowShift22a);
  res.json({ ok: true });
});

app.post('/api/alignment22a/reset', function(req, res) {
  previousAlignment22a = customAlignment22a ? JSON.parse(JSON.stringify(customAlignment22a)) : null;
  data.previousAlignment22a = previousAlignment22a;
  data.previousColShift22a = (typeof data.colShift22a === 'number') ? data.colShift22a : 0;
  data.previousRowShift22a = (typeof data.rowShift22a === 'number') ? data.rowShift22a : 0;
  customAlignment22a = null;
  delete data.customAlignment22a;
  data.colShift22a = 0;
  data.rowShift22a = 0;
  saveData(data);
  res.json({ ok: true });
});

app.post('/api/alignment22a/undo', function(req, res) {
  if (previousAlignment22a === null) {
    return res.json({ ok: false, message: 'No previous settings to restore' });
  }
  var temp = customAlignment22a ? JSON.parse(JSON.stringify(customAlignment22a)) : null;
  var tempCs = (typeof data.colShift22a === 'number') ? data.colShift22a : 0;
  var tempRs = (typeof data.rowShift22a === 'number') ? data.rowShift22a : 0;
  customAlignment22a = JSON.parse(JSON.stringify(previousAlignment22a));
  previousAlignment22a = temp;
  data.customAlignment22a = customAlignment22a;
  data.previousAlignment22a = previousAlignment22a;
  data.colShift22a = (typeof data.previousColShift22a === 'number') ? data.previousColShift22a : 0;
  data.rowShift22a = (typeof data.previousRowShift22a === 'number') ? data.previousRowShift22a : 0;
  data.previousColShift22a = tempCs;
  data.previousRowShift22a = tempRs;
  saveData(data);
  res.json({ ok: true, fields: getActiveRaw22aMap(), map: getActiveRaw22aMap(), colShift: data.colShift22a, rowShift: data.rowShift22a });
});

// Bake current alignment as new defaults (22A continuation page)
app.post('/api/alignment22a/bake-defaults', function(req, res) {
  var current = getActiveRaw22aMap();
  savedDefaults22a = JSON.parse(JSON.stringify(current));
  data.savedDefaults22a = savedDefaults22a;
  // Clear custom overrides since they're now baked into defaults
  previousAlignment22a = customAlignment22a ? JSON.parse(JSON.stringify(customAlignment22a)) : null;
  data.previousAlignment22a = previousAlignment22a;
  customAlignment22a = null;
  delete data.customAlignment22a;
  saveData(data);
  console.log('Alignment22a BAKED: savedDefaults22a now has ' + Object.keys(savedDefaults22a).length + ' fields');
  res.json({ ok: true, message: 'Current 22A settings saved as new defaults' });
});

// ==========================================
// HAZARDOUS WASTE LABEL SYSTEM
// ==========================================
// Label positions for 6"x6" pre-printed Labelmaster/Landsberg label (part #1174351)
// 72 cols at 12 CPI, 36 rows at 6 LPI
var LABEL_MAP = {
  dotShippingName1:  { row: 14, col: 3 },   // Fill area below "SHIPPING NAME" header
  dotShippingName2:  { row: 15, col: 3 },
  profileNumber:     { row: 16, col: 25 },  // Waste profile # centered below DOT name
  genPhone:          { row: 18, col: 42 },  // Right of "TELEPHONE"
  genName:           { row: 19, col: 10 },  // Right of "NAME"
  genAddress:        { row: 20, col: 14 },  // Right of "ADDRESS"
  genCityStateZip:   { row: 21, col: 10 },  // Right of "CITY"
  epaId:             { row: 24, col: 8 },   // Below "EPA ID#:"
  epaWasteNum:       { row: 24, col: 35 },  // Below "E.P.A. WASTE#:"
  epaWasteNum2:      { row: 25, col: 35 },  // Overflow waste codes (4th+)
  stateWasteCode:    { row: 25, col: 55 },  // Below "STATE WASTE CODE:"
  accumStartDate:    { row: 27, col: 20 },  // Right of "START DATE"
  manifestTrackNo:   { row: 27, col: 48 },  // Right of "TRACKING NO."
  contentsUN:        { row: 29, col: 20 },  // Large UN/NA number centered in contents area
  contents1:         { row: 30, col: 3 },   // Below "COMPOSITION"
  contents2:         { row: 31, col: 3 },
  physStateSolid:    { row: 32, col: 8 },   // Under "SOLID"
  physStateLiquid:   { row: 32, col: 16 },  // Under "LIQUID"
  physStateGas:      { row: 32, col: 24 },  // Under "GAS"
  hazPropFlammable:  { row: 31, col: 44 },  // Under "FLAMMABLE"
  hazPropCorrosive:  { row: 32, col: 32 },  // Under "CORROSIVE"
  hazPropReactivity: { row: 32, col: 44 },  // Under "REACTIVITY"
  hazPropToxic:      { row: 31, col: 58 },  // Under "TOXIC"
  hazPropOther:      { row: 32, col: 55 }   // Under "OTHER"
};

// Label alignment system (same pattern as 22A)
var savedDefaultsLabel = data.savedDefaultsLabel || null;
var customAlignmentLabel = data.customAlignmentLabel || null;
var previousAlignmentLabel = data.previousAlignmentLabel || null;

function getBaseLabelMap() {
  return savedDefaultsLabel || LABEL_MAP;
}

function getActiveLabelMap() {
  var base = getBaseLabelMap();
  if (!customAlignmentLabel) return base;
  var merged = {};
  var keys = Object.keys(base);
  for (var i = 0; i < keys.length; i++) {
    if (customAlignmentLabel[keys[i]]) {
      merged[keys[i]] = customAlignmentLabel[keys[i]];
    } else {
      merged[keys[i]] = base[keys[i]];
    }
  }
  return merged;
}

// Label alignment endpoints
app.get('/api/alignment-label', function(req, res) {
  customAlignmentLabel = data.customAlignmentLabel || null;
  previousAlignmentLabel = data.previousAlignmentLabel || null;
  savedDefaultsLabel = data.savedDefaultsLabel || null;
  res.json({
    fields: getActiveLabelMap(),
    map: getActiveLabelMap(),
    defaults: getBaseLabelMap(),
    colShift: (typeof data.colShiftLabel === 'number') ? data.colShiftLabel : 0,
    rowShift: (typeof data.rowShiftLabel === 'number') ? data.rowShiftLabel : 0,
    hasPrevious: !!previousAlignmentLabel,
    hasSavedDefaults: !!savedDefaultsLabel
  });
});

app.put('/api/alignment-label', function(req, res) {
  previousAlignmentLabel = customAlignmentLabel ? JSON.parse(JSON.stringify(customAlignmentLabel)) : null;
  data.previousAlignmentLabel = previousAlignmentLabel;
  data.previousColShiftLabel = (typeof data.colShiftLabel === 'number') ? data.colShiftLabel : 0;
  data.previousRowShiftLabel = (typeof data.rowShiftLabel === 'number') ? data.rowShiftLabel : 0;
  customAlignmentLabel = req.body.fields || null;
  data.customAlignmentLabel = customAlignmentLabel;
  if (typeof req.body.colShift === 'number') data.colShiftLabel = req.body.colShift;
  if (typeof req.body.rowShift === 'number') data.rowShiftLabel = req.body.rowShift;
  saveData(data);
  res.json({ ok: true, fields: getActiveLabelMap() });
});

app.post('/api/alignment-label/reset', function(req, res) {
  previousAlignmentLabel = customAlignmentLabel ? JSON.parse(JSON.stringify(customAlignmentLabel)) : null;
  data.previousAlignmentLabel = previousAlignmentLabel;
  data.previousColShiftLabel = (typeof data.colShiftLabel === 'number') ? data.colShiftLabel : 0;
  data.previousRowShiftLabel = (typeof data.rowShiftLabel === 'number') ? data.rowShiftLabel : 0;
  customAlignmentLabel = null;
  delete data.customAlignmentLabel;
  data.colShiftLabel = 0;
  data.rowShiftLabel = 0;
  saveData(data);
  res.json({ ok: true, fields: getActiveLabelMap() });
});

app.post('/api/alignment-label/undo', function(req, res) {
  if (!previousAlignmentLabel) return res.json({ ok: false, message: 'No previous settings' });
  var tempCs = (typeof data.colShiftLabel === 'number') ? data.colShiftLabel : 0;
  var tempRs = (typeof data.rowShiftLabel === 'number') ? data.rowShiftLabel : 0;
  customAlignmentLabel = JSON.parse(JSON.stringify(previousAlignmentLabel));
  data.customAlignmentLabel = customAlignmentLabel;
  previousAlignmentLabel = null;
  data.previousAlignmentLabel = null;
  data.colShiftLabel = (typeof data.previousColShiftLabel === 'number') ? data.previousColShiftLabel : 0;
  data.rowShiftLabel = (typeof data.previousRowShiftLabel === 'number') ? data.previousRowShiftLabel : 0;
  data.previousColShiftLabel = tempCs;
  data.previousRowShiftLabel = tempRs;
  saveData(data);
  res.json({ ok: true, fields: getActiveLabelMap(), map: getActiveLabelMap(), colShift: data.colShiftLabel, rowShift: data.rowShiftLabel });
});

app.post('/api/alignment-label/bake-defaults', function(req, res) {
  var current = getActiveLabelMap();
  savedDefaultsLabel = JSON.parse(JSON.stringify(current));
  data.savedDefaultsLabel = savedDefaultsLabel;
  previousAlignmentLabel = customAlignmentLabel ? JSON.parse(JSON.stringify(customAlignmentLabel)) : null;
  data.previousAlignmentLabel = previousAlignmentLabel;
  customAlignmentLabel = null;
  delete data.customAlignmentLabel;
  saveData(data);
  res.json({ ok: true, message: 'Current label settings saved as new defaults' });
});

// Non-Haz alignment system (same form as hazardous, separate alignment)
var savedDefaultsNonhaz = data.savedDefaultsNonhaz || null;
var customAlignmentNonhaz = data.customAlignmentNonhaz || null;
var previousAlignmentNonhaz = data.previousAlignmentNonhaz || null;

function getBaseNonhazMap() {
  return savedDefaultsNonhaz || FORM_8700_MAP;
}

function getActiveNonhazMap() {
  var base = getBaseNonhazMap();
  if (!customAlignmentNonhaz) return base;
  var merged = {};
  var keys = Object.keys(base);
  for (var i = 0; i < keys.length; i++) {
    if (customAlignmentNonhaz[keys[i]]) {
      merged[keys[i]] = customAlignmentNonhaz[keys[i]];
    } else {
      merged[keys[i]] = base[keys[i]];
    }
  }
  return merged;
}

// Non-Haz RAW map - compute deltas from FORM_8700_MAP to apply to RAW_MAP
// Same approach as getActiveRawMap() but using non-haz alignment data
function getActiveNonhazRawMap() {
  var base = RAW_MAP;
  if (!customAlignmentNonhaz) return base;
  var baseForm = getBaseNonhazMap();
  var merged = {};
  var keys = Object.keys(base);
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    if (customAlignmentNonhaz[key] && baseForm[key]) {
      var deltaRow = customAlignmentNonhaz[key].row - baseForm[key].row;
      var deltaCol = customAlignmentNonhaz[key].col - baseForm[key].col;
      merged[key] = { row: base[key].row + deltaRow, col: base[key].col + deltaCol };
    } else {
      merged[key] = base[key];
    }
  }
  return merged;
}

// Non-Haz 22A alignment (same base as hazardous 22A)
var savedDefaultsNonhaz22a = data.savedDefaultsNonhaz22a || null;
var customAlignmentNonhaz22a = data.customAlignmentNonhaz22a || null;

function getBaseNonhaz22aMap() {
  return savedDefaultsNonhaz22a || FORM_22A_MAP;
}

function getActiveNonhaz22aMap() {
  var base = getBaseNonhaz22aMap();
  if (!customAlignmentNonhaz22a) return base;
  var merged = {};
  var keys = Object.keys(base);
  for (var i = 0; i < keys.length; i++) {
    if (customAlignmentNonhaz22a[keys[i]]) {
      merged[keys[i]] = customAlignmentNonhaz22a[keys[i]];
    } else {
      merged[keys[i]] = base[keys[i]];
    }
  }
  return merged;
}

function getBaseNonhazRaw22aMap() {
  return data.savedDefaultsNonhazRaw22a || CONT_RAW_MAP;
}

function getActiveNonhazRaw22aMap() {
  var base = getBaseNonhazRaw22aMap();
  if (!data.customAlignmentNonhazRaw22a) return base;
  var merged = {};
  var keys = Object.keys(base);
  for (var i = 0; i < keys.length; i++) {
    if (data.customAlignmentNonhazRaw22a[keys[i]]) {
      merged[keys[i]] = data.customAlignmentNonhazRaw22a[keys[i]];
    } else {
      merged[keys[i]] = base[keys[i]];
    }
  }
  return merged;
}

// Non-Haz alignment endpoints
app.get('/api/alignment-nonhaz', function(req, res) {
  customAlignmentNonhaz = data.customAlignmentNonhaz || null;
  previousAlignmentNonhaz = data.previousAlignmentNonhaz || null;
  savedDefaultsNonhaz = data.savedDefaultsNonhaz || null;
  res.json({
    fields: getActiveNonhazMap(),
    map: getActiveNonhazMap(),
    defaults: getBaseNonhazMap(),
    colShift: (typeof data.colShiftNonhaz === 'number') ? data.colShiftNonhaz : 0,
    rowShift: (typeof data.rowShiftNonhaz === 'number') ? data.rowShiftNonhaz : 0,
    hasPrevious: !!previousAlignmentNonhaz,
    hasSavedDefaults: !!savedDefaultsNonhaz
  });
});

app.put('/api/alignment-nonhaz', function(req, res) {
  previousAlignmentNonhaz = customAlignmentNonhaz ? JSON.parse(JSON.stringify(customAlignmentNonhaz)) : null;
  data.previousAlignmentNonhaz = previousAlignmentNonhaz;
  data.previousColShiftNonhaz = (typeof data.colShiftNonhaz === 'number') ? data.colShiftNonhaz : 0;
  data.previousRowShiftNonhaz = (typeof data.rowShiftNonhaz === 'number') ? data.rowShiftNonhaz : 0;
  customAlignmentNonhaz = req.body.fields || null;
  data.customAlignmentNonhaz = customAlignmentNonhaz;
  if (typeof req.body.colShift === 'number') data.colShiftNonhaz = req.body.colShift;
  if (typeof req.body.rowShift === 'number') data.rowShiftNonhaz = req.body.rowShift;
  saveData(data);
  res.json({ ok: true, fields: getActiveNonhazMap() });
});

app.post('/api/alignment-nonhaz/reset', function(req, res) {
  previousAlignmentNonhaz = customAlignmentNonhaz ? JSON.parse(JSON.stringify(customAlignmentNonhaz)) : null;
  data.previousAlignmentNonhaz = previousAlignmentNonhaz;
  data.previousColShiftNonhaz = (typeof data.colShiftNonhaz === 'number') ? data.colShiftNonhaz : 0;
  data.previousRowShiftNonhaz = (typeof data.rowShiftNonhaz === 'number') ? data.rowShiftNonhaz : 0;
  customAlignmentNonhaz = null;
  delete data.customAlignmentNonhaz;
  data.colShiftNonhaz = 0;
  data.rowShiftNonhaz = 0;
  saveData(data);
  res.json({ ok: true, fields: getActiveNonhazMap() });
});

app.post('/api/alignment-nonhaz/undo', function(req, res) {
  if (!previousAlignmentNonhaz) return res.json({ ok: false, message: 'No previous settings' });
  var tempCs = (typeof data.colShiftNonhaz === 'number') ? data.colShiftNonhaz : 0;
  var tempRs = (typeof data.rowShiftNonhaz === 'number') ? data.rowShiftNonhaz : 0;
  customAlignmentNonhaz = JSON.parse(JSON.stringify(previousAlignmentNonhaz));
  data.customAlignmentNonhaz = customAlignmentNonhaz;
  previousAlignmentNonhaz = null;
  data.previousAlignmentNonhaz = null;
  data.colShiftNonhaz = (typeof data.previousColShiftNonhaz === 'number') ? data.previousColShiftNonhaz : 0;
  data.rowShiftNonhaz = (typeof data.previousRowShiftNonhaz === 'number') ? data.previousRowShiftNonhaz : 0;
  data.previousColShiftNonhaz = tempCs;
  data.previousRowShiftNonhaz = tempRs;
  saveData(data);
  res.json({ ok: true, fields: getActiveNonhazMap(), map: getActiveNonhazMap(), colShift: data.colShiftNonhaz, rowShift: data.rowShiftNonhaz });
});

app.post('/api/alignment-nonhaz/bake-defaults', function(req, res) {
  var current = getActiveNonhazMap();
  savedDefaultsNonhaz = JSON.parse(JSON.stringify(current));
  data.savedDefaultsNonhaz = savedDefaultsNonhaz;
  previousAlignmentNonhaz = customAlignmentNonhaz ? JSON.parse(JSON.stringify(customAlignmentNonhaz)) : null;
  data.previousAlignmentNonhaz = previousAlignmentNonhaz;
  customAlignmentNonhaz = null;
  delete data.customAlignmentNonhaz;
  saveData(data);
  res.json({ ok: true, message: 'Current non-haz settings saved as new defaults' });
});

// Label CRUD endpoints
app.get('/api/labels', function(req, res) {
  res.json(data.labels || []);
});

app.post('/api/labels', function(req, res) {
  var label = req.body;
  label.id = Date.now().toString();
  label.createdAt = new Date().toISOString();
  if (!data.labels) data.labels = [];
  data.labels.push(label);
  saveData(data);
  res.json(label);
});

app.put('/api/labels/:id', function(req, res) {
  var idx = -1;
  for (var i = 0; i < (data.labels || []).length; i++) {
    if (data.labels[i].id === req.params.id) { idx = i; break; }
  }
  if (idx === -1) return res.status(404).json({ error: 'Label not found' });
  data.labels[idx] = Object.assign(data.labels[idx], req.body);
  saveData(data);
  res.json(data.labels[idx]);
});

app.delete('/api/labels/:id', function(req, res) {
  data.labels = (data.labels || []).filter(function(l) { return l.id !== req.params.id; });
  saveData(data);
  res.json({ ok: true });
});

// Generate labels from a manifest (one per waste line)
app.post('/api/labels/from-manifest/:manifestId', function(req, res) {
  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.manifestId) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).json({ error: 'Manifest not found' });

  var wasteLineCount = parseInt(manifest.wasteLineCount) || 4;
  var labels = [];
  for (var w = 1; w <= wasteLineCount; w++) {
    var desc = manifest['waste' + w + 'Description'] || '';
    if (!desc.trim()) continue;

    // Build city/state/zip — use site location first, fall back to mailing
    var cityStateZip = manifest.genSiteCityStZip || manifest.generatorCityStZip || '';
    if (!cityStateZip) {
      if (manifest.generatorMailCity) cityStateZip += manifest.generatorMailCity;
      if (manifest.generatorMailState) cityStateZip += (cityStateZip ? ', ' : '') + manifest.generatorMailState;
      if (manifest.generatorMailZip) cityStateZip += ' ' + manifest.generatorMailZip;
    }

    // Parse waste codes
    var wasteCodes = (manifest['waste' + w + 'WasteCodes'] || '').trim();

    // Look up profile number from waste stream if profileId is set
    var profileNum = manifest['waste' + w + 'ProfileId'] || '';
    if (profileNum) {
      // Check if it matches a waste stream's profileId for the full number
      for (var ws = 0; ws < (data.wasteStreams || []).length; ws++) {
        if (data.wasteStreams[ws].id === profileNum || data.wasteStreams[ws].profileId === profileNum) {
          profileNum = data.wasteStreams[ws].profileId || profileNum;
          break;
        }
      }
    }

    var label = {
      id: Date.now().toString() + '-' + w,
      createdAt: new Date().toISOString(),
      manifestId: manifest.id,
      wasteLineNum: w,
      dotShippingName: desc,
      profileNumber: profileNum,
      genName: manifest.generatorName || '',
      genAddress: manifest.genSiteAddress || manifest.generatorAddress || '',
      genCityStateZip: cityStateZip,
      genPhone: manifest.generatorPhone || '',
      epaId: manifest.generatorEpaId || '',
      epaWasteNum: wasteCodes,
      stateWasteCode: '',
      accumStartDate: '',
      manifestTrackNo: manifest.manifestTrackingNum || manifest.manifestTrackingNumber || '',
      contents: '',
      physicalState: '',
      hazProps: { flammable: false, corrosive: false, reactivity: false, toxic: false, other: false }
    };
    labels.push(label);
    if (!data.labels) data.labels = [];
    data.labels.push(label);
  }
  saveData(data);
  res.json(labels);
});

// Print a single label (HTML positioned output for dot matrix)
app.get('/api/print/label/:id', function(req, res) {
  // Re-load data from disk to ensure we have the latest
  try {
    var freshData = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    Object.keys(freshData).forEach(function(k) { data[k] = freshData[k]; });
  } catch(e) { console.error('Label print: failed to re-read data:', e.message); }
  var label = null;
  for (var i = 0; i < (data.labels || []).length; i++) {
    if (data.labels[i].id === req.params.id) { label = data.labels[i]; break; }
  }
  if (!label) return res.status(404).send('Label not found');

  // Re-sync alignment
  customAlignmentLabel = data.customAlignmentLabel || null;
  var M = getActiveLabelMap();

  var CPI = 12;
  var LPI = 6;
  var BASE_LEFT_OFFSET = 0;
  var BASE_TOP_OFFSET = 0;
  var labelColShift = (typeof data.colShiftLabel === 'number') ? data.colShiftLabel : 0;
  var labelRowShift = (typeof data.rowShiftLabel === 'number') ? data.rowShiftLabel : 0;
  var colOffsetIn = BASE_LEFT_OFFSET + (labelColShift / CPI) + (parseFloat(req.query.colOffset) || 0);
  var rowOffsetIn = BASE_TOP_OFFSET + (labelRowShift / LPI) + (parseFloat(req.query.rowOffset) || 0);

  var placements = [];

  function place(fieldKey, text) {
    if (!text || !M[fieldKey]) return;
    placements.push({ row: M[fieldKey].row, col: M[fieldKey].col, text: String(text) });
  }

  // DOT Shipping Name (wrap to 2 lines at ~60 chars)
  var dotName = (label.dotShippingName || '').trim();
  if (dotName.length > 60) {
    // Word wrap
    var words = dotName.split(' ');
    var line1 = '';
    var lineIdx = 0;
    for (var wi = 0; wi < words.length; wi++) {
      if (line1.length + words[wi].length + 1 > 60 && line1.length > 0) {
        lineIdx = wi;
        break;
      }
      line1 += (line1 ? ' ' : '') + words[wi];
      lineIdx = wi + 1;
    }
    var line2 = words.slice(lineIdx).join(' ');
    place('dotShippingName1', line1);
    place('dotShippingName2', line2);
  } else {
    place('dotShippingName1', dotName);
  }

  // Generator info
  place('genName', label.genName);
  place('genAddress', label.genAddress);
  place('genCityStateZip', label.genCityStateZip);
  place('genPhone', label.genPhone);

  // Profile number (below DOT shipping name, centered)
  place('profileNumber', label.profileNumber ? 'Profile # ' + label.profileNumber : '');

  // EPA / Waste - split codes: first 3 on main line, rest on overflow line
  place('epaId', label.epaId);
  var allWasteCodes = (label.epaWasteNum || '').trim();
  if (allWasteCodes) {
    var codeList = allWasteCodes.replace(/([A-Za-z]\d{3})/g, ' $1').replace(/\s+/g, ' ').trim().split(' ').filter(Boolean);
    if (codeList.length > 3) {
      place('epaWasteNum', codeList.slice(0, 3).join(' '));
      place('epaWasteNum2', codeList.slice(3).join(' '));
    } else {
      place('epaWasteNum', allWasteCodes);
    }
  }
  place('stateWasteCode', label.stateWasteCode);

  // Dates / Manifest
  place('accumStartDate', label.accumStartDate);
  // Manifest tracking number - fall back to manifest record if label doesn't have it
  var labelTrackNo = label.manifestTrackNo || '';
  if (!labelTrackNo && label.manifestId) {
    for (var mti = 0; mti < (data.manifests || []).length; mti++) {
      if (data.manifests[mti].id === label.manifestId) {
        labelTrackNo = data.manifests[mti].manifestTrackingNum || data.manifests[mti].manifestTrackingNumber || '';
        break;
      }
    }
  }
  if (labelTrackNo && M.manifestTrackNo) {
    placements.push({ row: M.manifestTrackNo.row, col: M.manifestTrackNo.col, text: String(labelTrackNo), medium: true });
  }

  // Extract UN/NA number from DOT shipping name for large display in contents area
  var unNumber = '';
  var dotText = label.dotShippingName || '';
  var unMatch = dotText.match(/\b(UN|NA)\s*(\d{4})\b/i);
  if (unMatch) {
    unNumber = unMatch[1].toUpperCase() + unMatch[2];
  }
  // Place large UN/NA number centered in contents area (rendered with larger font)
  if (unNumber && M.contentsUN) {
    placements.push({ row: M.contentsUN.row, col: M.contentsUN.col, text: unNumber, large: true });
  }

  // Contents (wrap to 2 lines at ~60 chars)
  var contents = (label.contents || '').trim();
  if (contents) {
    var cWords = contents.split(' ');
    var cLines = [''];
    var cLineNum = 0;
    for (var ci = 0; ci < cWords.length && cLineNum < 2; ci++) {
      if (cLines[cLineNum].length + cWords[ci].length + 1 > 60 && cLines[cLineNum].length > 0) {
        cLineNum++;
        if (cLineNum >= 2) break;
        cLines[cLineNum] = '';
      }
      cLines[cLineNum] += (cLines[cLineNum] ? ' ' : '') + cWords[ci];
    }
    if (cLines[0]) place('contents1', cLines[0]);
    if (cLines[1]) place('contents2', cLines[1]);
  }

  // Physical State checkboxes
  if (label.physicalState === 'solid') place('physStateSolid', 'X');
  if (label.physicalState === 'liquid') place('physStateLiquid', 'X');
  if (label.physicalState === 'gas') place('physStateGas', 'X');

  // Hazardous Properties checkboxes
  var hp = label.hazProps || {};
  if (hp.flammable) place('hazPropFlammable', 'X');
  if (hp.corrosive) place('hazPropCorrosive', 'X');
  if (hp.reactivity) place('hazPropReactivity', 'X');
  if (hp.toxic) place('hazPropToxic', 'X');
  if (hp.other) place('hazPropOther', 'X');

  // Build HTML output - continuous feed (no page breaks) for pinfeed dot matrix
  var copies = parseInt(req.query.copies) || 1;
  if (copies < 1) copies = 1;
  if (copies > 100) copies = 100;
  var totalHeight = 6 * copies;

  var html = '<!DOCTYPE html><html><head><title>Print Label</title><style>';
  html += '@page { margin: 0; size: 6in ' + totalHeight + 'in; }';
  html += '@media print { body { margin: 0; padding: 0; } .no-print { display: none !important; } }';
  html += 'body { margin: 0; padding: 0; }';
  html += '.sheet { position: relative; width: 6in; height: ' + totalHeight + 'in; }';
  html += '.field { position: absolute; font-family: "Courier New", Courier, monospace; font-size: 10pt; font-weight: bold; line-height: 1; white-space: pre; margin: 0; padding: 0; }';
  html += '.toolbar { padding: 10px; background: #f0f0f0; text-align: center; font-family: sans-serif; }';
  html += '.toolbar button { padding: 8px 20px; font-size: 16px; margin: 0 5px; cursor: pointer; }';
  html += '.toolbar .print-btn { background: #f59e0b; color: white; border: none; border-radius: 4px; }';
  html += '.toolbar .close-btn { background: #6b7280; color: white; border: none; border-radius: 4px; }';
  html += '</style></head><body>';

  html += '<div class="no-print toolbar">';
  html += '<button class="print-btn" onclick="window.print()">Print ' + copies + ' Label' + (copies > 1 ? 's' : '') + '</button>';
  html += '<button class="close-btn" onclick="window.close()">Close</button>';
  html += '<span style="margin-left:20px;font-size:12px;color:#666">6x6 Hazardous Waste Label - Epson LQ-590II (' + copies + ' copies). Set paper size to 6x' + totalHeight + ' and margins to None.</span>';
  html += '</div>';

  html += '<div class="sheet">';
  for (var ci = 0; ci < copies; ci++) {
    var pageOffsetIn = ci * 6;
    for (var pi = 0; pi < placements.length; pi++) {
      var p = placements[pi];
      var leftIn = ((p.col - 1) / CPI) + colOffsetIn;
      var topIn = ((p.row - 1) / LPI) + rowOffsetIn + pageOffsetIn;
      var safeText = p.text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      if (p.large) {
        html += '<span class="field" style="left:' + leftIn.toFixed(4) + 'in;top:' + topIn.toFixed(4) + 'in;font-size:36pt;font-weight:bold;letter-spacing:2px;">' + safeText + '</span>';
      } else if (p.medium) {
        html += '<span class="field" style="left:' + leftIn.toFixed(4) + 'in;top:' + topIn.toFixed(4) + 'in;font-size:14pt;font-weight:bold;">' + safeText + '</span>';
      } else {
        html += '<span class="field" style="left:' + leftIn.toFixed(4) + 'in;top:' + topIn.toFixed(4) + 'in;">' + safeText + '</span>';
      }
    }
  }
  html += '</div>';
  html += '</body></html>';
  res.type('html').send(html);
});

// Batch print all labels for a manifest
app.get('/api/print/labels/manifest/:manifestId', function(req, res) {
  // Re-load data from disk to ensure we have the latest
  try {
    var freshData = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    Object.keys(freshData).forEach(function(k) { data[k] = freshData[k]; });
  } catch(e) { console.error('Batch label print: failed to re-read data:', e.message); }
  var manifestLabels = (data.labels || []).filter(function(l) { return l.manifestId === req.params.manifestId; });
  if (manifestLabels.length === 0) return res.status(404).send('No labels found for this manifest');

  customAlignmentLabel = data.customAlignmentLabel || null;
  var M = getActiveLabelMap();

  var CPI = 12;
  var LPI = 6;
  var labelColShiftB = (typeof data.colShiftLabel === 'number') ? data.colShiftLabel : 0;
  var labelRowShiftB = (typeof data.rowShiftLabel === 'number') ? data.rowShiftLabel : 0;
  var colOffsetIn = (labelColShiftB / CPI) + (parseFloat(req.query.colOffset) || 0);
  var rowOffsetIn = (labelRowShiftB / LPI) + (parseFloat(req.query.rowOffset) || 0);

  var totalHeight = 6 * manifestLabels.length;

  var html = '<!DOCTYPE html><html><head><title>Print Labels</title><style>';
  html += '@page { margin: 0; size: 6in ' + totalHeight + 'in; }';
  html += '@media print { body { margin: 0; padding: 0; } .no-print { display: none !important; } }';
  html += 'body { margin: 0; padding: 0; }';
  html += '.sheet { position: relative; width: 6in; height: ' + totalHeight + 'in; }';
  html += '.field { position: absolute; font-family: "Courier New", Courier, monospace; font-size: 10pt; font-weight: bold; line-height: 1; white-space: pre; margin: 0; padding: 0; }';
  html += '.toolbar { padding: 10px; background: #f0f0f0; text-align: center; font-family: sans-serif; }';
  html += '.toolbar button { padding: 8px 20px; font-size: 16px; margin: 0 5px; cursor: pointer; }';
  html += '.toolbar .print-btn { background: #f59e0b; color: white; border: none; border-radius: 4px; }';
  html += '.toolbar .close-btn { background: #6b7280; color: white; border: none; border-radius: 4px; }';
  html += '</style></head><body>';

  html += '<div class="no-print toolbar">';
  html += '<button class="print-btn" onclick="window.print()">Print All Labels (' + manifestLabels.length + ')</button>';
  html += '<button class="close-btn" onclick="window.close()">Close</button>';
  html += '<span style="margin-left:20px;font-size:12px;color:#666">Batch labels - Epson LQ-590II (' + manifestLabels.length + ' labels). Set paper size to 6x' + totalHeight + ' and margins to None.</span>';
  html += '</div>';

  html += '<div class="sheet">';

  for (var li = 0; li < manifestLabels.length; li++) {
    var label = manifestLabels[li];
    var placements = [];

    function placeB(fieldKey, text) {
      if (!text || !M[fieldKey]) return;
      placements.push({ row: M[fieldKey].row, col: M[fieldKey].col, text: String(text) });
    }

    // DOT Shipping Name
    var dotName = (label.dotShippingName || '').trim();
    if (dotName.length > 60) {
      var words = dotName.split(' ');
      var line1 = '';
      var lineIdx = 0;
      for (var wi = 0; wi < words.length; wi++) {
        if (line1.length + words[wi].length + 1 > 60 && line1.length > 0) { lineIdx = wi; break; }
        line1 += (line1 ? ' ' : '') + words[wi];
        lineIdx = wi + 1;
      }
      placeB('dotShippingName1', line1);
      placeB('dotShippingName2', words.slice(lineIdx).join(' '));
    } else {
      placeB('dotShippingName1', dotName);
    }

    placeB('genName', label.genName);
    placeB('genAddress', label.genAddress);
    placeB('genCityStateZip', label.genCityStateZip);
    placeB('genPhone', label.genPhone);
    placeB('profileNumber', label.profileNumber ? 'Profile # ' + label.profileNumber : '');
    placeB('epaId', label.epaId);
    var bAllWC = (label.epaWasteNum || '').trim();
    if (bAllWC) {
      var bCodes = bAllWC.replace(/([A-Za-z]\d{3})/g, ' $1').replace(/\s+/g, ' ').trim().split(' ').filter(Boolean);
      if (bCodes.length > 3) {
        placeB('epaWasteNum', bCodes.slice(0, 3).join(' '));
        placeB('epaWasteNum2', bCodes.slice(3).join(' '));
      } else {
        placeB('epaWasteNum', bAllWC);
      }
    }
    placeB('stateWasteCode', label.stateWasteCode);
    placeB('accumStartDate', label.accumStartDate);
    // Manifest tracking number - fall back to manifest record if label doesn't have it
    var bTrackNo = label.manifestTrackNo || '';
    if (!bTrackNo && label.manifestId) {
      for (var mti = 0; mti < (data.manifests || []).length; mti++) {
        if (data.manifests[mti].id === label.manifestId) {
          bTrackNo = data.manifests[mti].manifestTrackingNum || data.manifests[mti].manifestTrackingNumber || '';
          break;
        }
      }
    }
    if (bTrackNo && M.manifestTrackNo) {
      placements.push({ row: M.manifestTrackNo.row, col: M.manifestTrackNo.col, text: String(bTrackNo), medium: true });
    }

    // Extract UN/NA number for large display
    var bUnNumber = '';
    var bDotText = label.dotShippingName || '';
    var bUnMatch = bDotText.match(/\b(UN|NA)\s*(\d{4})\b/i);
    if (bUnMatch) {
      bUnNumber = bUnMatch[1].toUpperCase() + bUnMatch[2];
    }
    if (bUnNumber && M.contentsUN) {
      placements.push({ row: M.contentsUN.row, col: M.contentsUN.col, text: bUnNumber, large: true });
    }

    // Contents (wrap to 2 lines)
    var contents = (label.contents || '').trim();
    if (contents) {
      var cWords = contents.split(' ');
      var cLines = [''];
      var cLineNum = 0;
      for (var ci = 0; ci < cWords.length && cLineNum < 2; ci++) {
        if (cLines[cLineNum].length + cWords[ci].length + 1 > 60 && cLines[cLineNum].length > 0) {
          cLineNum++;
          if (cLineNum >= 2) break;
          cLines[cLineNum] = '';
        }
        cLines[cLineNum] += (cLines[cLineNum] ? ' ' : '') + cWords[ci];
      }
      if (cLines[0]) placeB('contents1', cLines[0]);
      if (cLines[1]) placeB('contents2', cLines[1]);
    }

    if (label.physicalState === 'solid') placeB('physStateSolid', 'X');
    if (label.physicalState === 'liquid') placeB('physStateLiquid', 'X');
    if (label.physicalState === 'gas') placeB('physStateGas', 'X');

    var hp = label.hazProps || {};
    if (hp.flammable) placeB('hazPropFlammable', 'X');
    if (hp.corrosive) placeB('hazPropCorrosive', 'X');
    if (hp.reactivity) placeB('hazPropReactivity', 'X');
    if (hp.toxic) placeB('hazPropToxic', 'X');
    if (hp.other) placeB('hazPropOther', 'X');

    var labelOffsetIn = li * 6;
    for (var pi = 0; pi < placements.length; pi++) {
      var p = placements[pi];
      var leftIn = ((p.col - 1) / CPI) + colOffsetIn;
      var topIn = ((p.row - 1) / LPI) + rowOffsetIn + labelOffsetIn;
      var safeText = p.text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      if (p.large) {
        html += '<span class="field" style="left:' + leftIn.toFixed(4) + 'in;top:' + topIn.toFixed(4) + 'in;font-size:36pt;font-weight:bold;letter-spacing:2px;">' + safeText + '</span>';
      } else if (p.medium) {
        html += '<span class="field" style="left:' + leftIn.toFixed(4) + 'in;top:' + topIn.toFixed(4) + 'in;font-size:14pt;font-weight:bold;">' + safeText + '</span>';
      } else {
        html += '<span class="field" style="left:' + leftIn.toFixed(4) + 'in;top:' + topIn.toFixed(4) + 'in;">' + safeText + '</span>';
      }
    }
  }

  html += '</div>';
  html += '</body></html>';
  res.type('html').send(html);
});

// ============================================================
// BILL OF LADING (BOL) SYSTEM
// ============================================================

// BOL_MAP — Field positions for 8.5"x11" Labelmaster F375-3 Straight BOL
// CPI=10 (85 cols across 8.5"), LPI=6 (66 rows across 11")
// Positions are initial estimates — calibrate with alignment editor + test prints
var BOL_MAP = {
  // Header
  date:               { row: 9, col: 70 },

  // Page / Carrier
  pageNum:            { row: 11, col: 9 },
  pageOf:             { row: 11, col: 16 },
  carrierName:        { row: 11, col: 33 },
  scac:               { row: 11, col: 60 },

  // TO (Consignee) — left column
  toConsignee:        { row: 13, col: 3 },
  toStreet:           { row: 14, col: 3 },
  toCity:             { row: 15, col: 3 },
  toState:            { row: 15, col: 23 },
  toZip:              { row: 15, col: 33 },

  // FROM (Shipper) — right column
  fromShipper:        { row: 13, col: 46 },
  fromStreet:         { row: 14, col: 46 },
  fromCity:           { row: 15, col: 46 },
  fromState:          { row: 15, col: 63 },
  fromZip:            { row: 15, col: 70 },
  emergencyPhone:     { row: 16, col: 58 },

  // BOL Number
  bolNumber:          { row: 17, col: 6 },

  // Line items (14 rows, 2 rows each on form; desc uses CSS wrapping)
  // Columns: units(col 1), hm(col 12), desc(col 15, CSS-wrapped), qty(col 53), weight(col 63)
  line1units: { row: 20, col: 3 },  line1hm: { row: 20, col: 12 }, line1desc: { row: 20, col: 15 }, line1qty: { row: 20, col: 53 }, line1weight: { row: 20, col: 63 },
  line2units: { row: 22, col: 3 },  line2hm: { row: 22, col: 12 }, line2desc: { row: 22, col: 15 }, line2qty: { row: 22, col: 53 }, line2weight: { row: 22, col: 63 },
  line3units: { row: 24, col: 3 },  line3hm: { row: 24, col: 12 }, line3desc: { row: 24, col: 15 }, line3qty: { row: 24, col: 53 }, line3weight: { row: 24, col: 63 },
  line4units: { row: 26, col: 3 },  line4hm: { row: 26, col: 12 }, line4desc: { row: 26, col: 15 }, line4qty: { row: 26, col: 53 }, line4weight: { row: 26, col: 63 },
  line5units: { row: 28, col: 3 },  line5hm: { row: 28, col: 12 }, line5desc: { row: 28, col: 15 }, line5qty: { row: 28, col: 53 }, line5weight: { row: 28, col: 63 },
  line6units: { row: 30, col: 3 },  line6hm: { row: 30, col: 12 }, line6desc: { row: 30, col: 15 }, line6qty: { row: 30, col: 53 }, line6weight: { row: 30, col: 63 },
  line7units: { row: 32, col: 3 },  line7hm: { row: 32, col: 12 }, line7desc: { row: 32, col: 15 }, line7qty: { row: 32, col: 53 }, line7weight: { row: 32, col: 63 },
  line8units: { row: 34, col: 3 },  line8hm: { row: 34, col: 12 }, line8desc: { row: 34, col: 15 }, line8qty: { row: 34, col: 53 }, line8weight: { row: 34, col: 63 },
  line9units: { row: 36, col: 3 },  line9hm: { row: 36, col: 12 }, line9desc: { row: 36, col: 15 }, line9qty: { row: 36, col: 53 }, line9weight: { row: 36, col: 63 },
  line10units: { row: 38, col: 1 }, line10hm: { row: 38, col: 12 }, line10desc: { row: 38, col: 15 }, line10qty: { row: 38, col: 53 }, line10weight: { row: 38, col: 63 },
  line11units: { row: 40, col: 1 }, line11hm: { row: 40, col: 12 }, line11desc: { row: 40, col: 15 }, line11qty: { row: 40, col: 53 }, line11weight: { row: 40, col: 63 },
  line12units: { row: 42, col: 1 }, line12hm: { row: 42, col: 12 }, line12desc: { row: 42, col: 15 }, line12qty: { row: 42, col: 53 }, line12weight: { row: 42, col: 63 },
  line13units: { row: 44, col: 1 }, line13hm: { row: 44, col: 12 }, line13desc: { row: 44, col: 15 }, line13qty: { row: 44, col: 53 }, line13weight: { row: 44, col: 63 },
  line14units: { row: 46, col: 1 }, line14hm: { row: 46, col: 12 }, line14desc: { row: 46, col: 15 }, line14qty: { row: 46, col: 53 }, line14weight: { row: 46, col: 63 },

  // Bottom signature area
  bottomShipper:      { row: 56, col: 3 },
  bottomCarrier:      { row: 56, col: 46 }
};

// BOL Alignment System — same pattern as labels
var savedDefaultsBol = data.savedDefaultsBol || null;
var customAlignmentBol = data.customAlignmentBol || null;
var previousAlignmentBol = data.previousAlignmentBol || null;

function getBaseBolMap() {
  return savedDefaultsBol || BOL_MAP;
}

function getActiveBolMap() {
  var base = getBaseBolMap();
  if (!customAlignmentBol) return JSON.parse(JSON.stringify(base));
  var merged = {};
  var keys = Object.keys(base);
  for (var i = 0; i < keys.length; i++) {
    merged[keys[i]] = customAlignmentBol[keys[i]] || base[keys[i]];
  }
  return merged;
}

// Initialize BOL array
if (!data.bols) { data.bols = []; saveData(data); }

// ---- BOL CRUD Endpoints ----

app.get('/api/bols', function(req, res) {
  res.json(data.bols || []);
});

app.post('/api/bols', function(req, res) {
  var bol = req.body;
  bol.id = Date.now().toString();
  bol.createdAt = new Date().toISOString();
  if (!data.bols) data.bols = [];
  data.bols.push(bol);
  saveData(data);
  res.json(bol);
});

app.put('/api/bols/:id', function(req, res) {
  var idx = -1;
  for (var i = 0; i < (data.bols || []).length; i++) {
    if (data.bols[i].id === req.params.id) { idx = i; break; }
  }
  if (idx === -1) return res.status(404).json({ error: 'BOL not found' });
  data.bols[idx] = Object.assign(data.bols[idx], req.body);
  saveData(data);
  res.json(data.bols[idx]);
});

app.delete('/api/bols/:id', function(req, res) {
  data.bols = (data.bols || []).filter(function(b) { return b.id !== req.params.id; });
  saveData(data);
  res.json({ ok: true });
});

// ---- Generate BOL from Manifest ----

app.post('/api/bols/from-manifest/:manifestId', function(req, res) {
  var manifest = null;
  for (var i = 0; i < (data.manifests || []).length; i++) {
    if (data.manifests[i].id === req.params.manifestId) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).json({ error: 'Manifest not found' });

  var bol = {
    id: Date.now().toString(),
    createdAt: new Date().toISOString(),
    manifestId: manifest.id,
    date: new Date().toLocaleDateString('en-US'),
    pageNum: '1',
    pageOf: '1',
    carrierName: manifest.transporter1Company || 'Independence Environmental Services, LLC',
    scac: '',
    toConsignee: manifest.facilityName || '',
    toStreet: manifest.facilityAddress || '',
    toCity: manifest.facilityCity || '',
    toState: manifest.facilityState || '',
    toZip: manifest.facilityZip || '',
    fromShipper: manifest.generatorName || '',
    fromStreet: manifest.generatorMailAddr || manifest.generatorSiteAddr || '',
    fromCity: manifest.generatorMailCity || manifest.generatorCity || '',
    fromState: manifest.generatorMailState || manifest.generatorState || '',
    fromZip: manifest.generatorMailZip || manifest.generatorZip || '',
    emergencyPhone: manifest.emergencyPhone || '',
    bolNumber: '',
    lines: []
  };

  // Map waste lines to BOL line items
  for (var w = 1; w <= 4; w++) {
    var desc = manifest['waste' + w + 'Description'] || '';
    if (!desc) continue;
    var qty = manifest['waste' + w + 'Qty'] || '';
    var containerNum = manifest['waste' + w + 'ContainerNum'] || '';
    var containerType = manifest['waste' + w + 'ContainerType'] || '';
    var unit = manifest['waste' + w + 'Unit'] || '';
    var hm = manifest['waste' + w + 'HM'] || 'X';

    var containerSize = manifest['waste' + w + 'ContainerSize'] || '';
    bol.lines.push({
      containerNum: containerNum,
      containerType: containerType,
      containerSize: containerSize,
      hm: hm,
      desc: desc,
      qty: qty + (unit ? ' ' + unit : ''),
      weight: ''
    });
  }

  // Pad lines array to 14
  while (bol.lines.length < 14) {
    bol.lines.push({ containerNum: '', containerType: '', containerSize: '', hm: '', desc: '', qty: '', weight: '' });
  }

  if (!data.bols) data.bols = [];
  data.bols.push(bol);
  saveData(data);
  res.json(bol);
});

// ---- BOL Alignment Endpoints ----

app.get('/api/alignment-bol', function(req, res) {
  customAlignmentBol = data.customAlignmentBol || null;
  previousAlignmentBol = data.previousAlignmentBol || null;
  savedDefaultsBol = data.savedDefaultsBol || null;
  res.json({
    fields: getActiveBolMap(),
    defaults: getBaseBolMap(),
    hasPrevious: !!previousAlignmentBol,
    hasSavedDefaults: !!savedDefaultsBol
  });
});

app.put('/api/alignment-bol', function(req, res) {
  previousAlignmentBol = customAlignmentBol ? JSON.parse(JSON.stringify(customAlignmentBol)) : null;
  data.previousAlignmentBol = previousAlignmentBol;
  customAlignmentBol = req.body.fields || null;
  data.customAlignmentBol = customAlignmentBol;
  saveData(data);
  res.json({ ok: true, fields: getActiveBolMap() });
});

app.post('/api/alignment-bol/reset', function(req, res) {
  previousAlignmentBol = customAlignmentBol ? JSON.parse(JSON.stringify(customAlignmentBol)) : null;
  data.previousAlignmentBol = previousAlignmentBol;
  customAlignmentBol = null;
  data.customAlignmentBol = null;
  saveData(data);
  res.json({ ok: true, fields: getActiveBolMap() });
});

app.post('/api/alignment-bol/undo', function(req, res) {
  if (!previousAlignmentBol) return res.status(400).json({ error: 'No previous state' });
  customAlignmentBol = JSON.parse(JSON.stringify(previousAlignmentBol));
  data.customAlignmentBol = customAlignmentBol;
  previousAlignmentBol = null;
  data.previousAlignmentBol = null;
  saveData(data);
  res.json({ ok: true, fields: getActiveBolMap() });
});

app.post('/api/alignment-bol/copy-line1', function(req, res) {
  var active = getActiveBolMap();
  var base = getBaseBolMap();
  // Calculate line 1 offsets from base
  var suffixes = ['units', 'hm', 'desc', 'qty', 'weight'];
  var offsets = {};
  for (var si = 0; si < suffixes.length; si++) {
    var key1 = 'line1' + suffixes[si];
    if (active[key1] && base[key1]) {
      offsets[suffixes[si]] = {
        rowOff: active[key1].row - base[key1].row,
        colOff: active[key1].col - base[key1].col
      };
    }
  }
  // Apply those offsets to lines 2-14
  previousAlignmentBol = customAlignmentBol ? JSON.parse(JSON.stringify(customAlignmentBol)) : null;
  data.previousAlignmentBol = previousAlignmentBol;
  var newFields = {};
  var keys = Object.keys(active);
  for (var ki = 0; ki < keys.length; ki++) {
    newFields[keys[ki]] = { row: active[keys[ki]].row, col: active[keys[ki]].col };
  }
  for (var ln = 2; ln <= 14; ln++) {
    for (var sj = 0; sj < suffixes.length; sj++) {
      var fieldKey = 'line' + ln + suffixes[sj];
      var baseKey = fieldKey;
      if (newFields[fieldKey] && base[baseKey] && offsets[suffixes[sj]]) {
        newFields[fieldKey].row = base[baseKey].row + offsets[suffixes[sj]].rowOff;
        newFields[fieldKey].col = base[baseKey].col + offsets[suffixes[sj]].colOff;
      }
    }
  }
  customAlignmentBol = newFields;
  data.customAlignmentBol = customAlignmentBol;
  saveData(data);
  res.json({ ok: true, fields: getActiveBolMap() });
});

app.post('/api/alignment-bol/bake-defaults', function(req, res) {
  savedDefaultsBol = JSON.parse(JSON.stringify(getActiveBolMap()));
  data.savedDefaultsBol = savedDefaultsBol;
  customAlignmentBol = null;
  data.customAlignmentBol = null;
  previousAlignmentBol = null;
  data.previousAlignmentBol = null;
  saveData(data);
  res.json({ ok: true, fields: getActiveBolMap() });
});

// ---- BOL Print Endpoints ----

app.get('/api/print/bol/:id', function(req, res) {
  // Re-load data from disk to ensure we have the latest
  try {
    var freshData = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    Object.keys(freshData).forEach(function(k) { data[k] = freshData[k]; });
  } catch(e) { console.error('BOL print: failed to re-read data:', e.message); }
  var bol = null;
  for (var i = 0; i < (data.bols || []).length; i++) {
    if (data.bols[i].id === req.params.id) { bol = data.bols[i]; break; }
  }
  if (!bol) return res.status(404).send('BOL not found');

  // Auto-file BOL as printed
  fileBolAsPrinted(bol.id);

  customAlignmentBol = data.customAlignmentBol || null;
  savedDefaultsBol = data.savedDefaultsBol || null;
  var M = getActiveBolMap();
  var CPI = 10;
  var LPI = 6;
  var colOffsetIn = parseFloat(req.query.colOffset) || 0;
  var rowOffsetIn = parseFloat(req.query.rowOffset) || 0;

  var placements = [];
  function place(fieldKey, text) {
    if (!text || !M[fieldKey]) return;
    placements.push({ row: M[fieldKey].row, col: M[fieldKey].col, text: String(text), fieldKey: fieldKey });
  }

  // Header
  place('date', bol.date);
  place('pageNum', bol.pageNum);
  place('pageOf', bol.pageOf);
  place('carrierName', bol.carrierName);
  place('scac', bol.scac);

  // TO (Consignee)
  place('toConsignee', bol.toConsignee);
  place('toStreet', bol.toStreet);
  place('toCity', bol.toCity);
  place('toState', bol.toState);
  place('toZip', bol.toZip);

  // FROM (Shipper)
  place('fromShipper', bol.fromShipper);
  place('fromStreet', bol.fromStreet);
  place('fromCity', bol.fromCity);
  place('fromState', bol.fromState);
  place('fromZip', bol.fromZip);
  place('emergencyPhone', bol.emergencyPhone);

  // Route
  // BOL Number - prints "BOL # " prefix
  place('bolNumber', bol.bolNumber ? 'BOL # ' + bol.bolNumber : '');

  // Line items
  var lines = bol.lines || [];
  for (var ln = 0; ln < 14; ln++) {
    var line = lines[ln] || {};
    var n = ln + 1;
    var unitsText = [line.containerNum || '', line.containerType || ''].filter(Boolean).join('/');
    place('line' + n + 'units', unitsText);
    place('line' + n + 'hm', line.hm);
    // Description - append (size/type) at end
    var descText = (line.desc || '').trim();
    var sizeNum = line.containerSize ? line.containerSize.replace(/[^0-9]/g, '') : '';
    if (sizeNum && line.containerType) {
      descText = descText ? descText + ' (' + sizeNum + '/' + line.containerType + ')' : '(' + sizeNum + '/' + line.containerType + ')';
    }
    place('line' + n + 'desc', descText);
    place('line' + n + 'qty', line.qty);
    place('line' + n + 'weight', line.weight);
  }

  // Bottom signature area — auto-populate
  place('bottomShipper', bol.fromShipper || '');
  place('bottomCarrier', 'Independence Environmental Services');

  // Build HTML
  var html = '<!DOCTYPE html><html><head><title>Print BOL</title><style>';
  html += '@page { margin: 0; size: 8.5in 11in; }';
  html += '@media print { body { margin: 0; padding: 0; } .no-print { display: none !important; } }';
  html += 'body { margin: 0; padding: 0; }';
  html += '.page { position: relative; width: 8.5in; height: 11in; overflow: hidden; page-break-after: always; }';
  html += '.page:last-child { page-break-after: auto; }';
  html += '.field { position: absolute; font-family: "Courier New", Courier, monospace; font-size: 10pt; font-weight: bold; line-height: 1; white-space: pre; margin: 0; padding: 0; }';
  html += '.field-wrap { position: absolute; font-family: "Courier New", Courier, monospace; font-size: 10pt; font-weight: bold; line-height: 1.15; white-space: pre-wrap; word-break: break-word; margin: 0; padding: 0; }';
  html += '.toolbar { padding: 10px; background: #f0f0f0; text-align: center; font-family: sans-serif; }';
  html += '.toolbar button { padding: 8px 20px; font-size: 16px; margin: 0 5px; cursor: pointer; }';
  html += '.toolbar .print-btn { background: #2563eb; color: white; border: none; border-radius: 4px; }';
  html += '.toolbar .close-btn { background: #6b7280; color: white; border: none; border-radius: 4px; }';
  html += '</style></head><body>';

  html += '<div class="no-print toolbar">';
  html += '<button class="print-btn" onclick="window.print()">Print BOL</button>';
  html += '<button class="close-btn" onclick="window.close()">Close</button>';
  html += '<span style="margin-left:20px;font-size:12px;color:#666">8.5x11 Straight Bill of Lading - Epson LQ-590II. Set paper size to 8.5x11 and margins to None.</span>';
  html += '</div>';

  // Calculate max width for description fields: from col 15 to col 53 = 38 chars at CPI 10
  var descWidthIn = 38 / CPI;

  html += '<div class="page">';
  for (var pi = 0; pi < placements.length; pi++) {
    var p = placements[pi];
    var leftIn = ((p.col - 1) / CPI) + colOffsetIn;
    var topIn = ((p.row - 1) / LPI) + rowOffsetIn;
    var safeText = p.text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    // Use wrapping class for description fields, smaller font for units
    if (p.fieldKey && p.fieldKey.match(/line\d+desc$/)) {
      html += '<span class="field-wrap" style="left:' + leftIn.toFixed(4) + 'in;top:' + topIn.toFixed(4) + 'in;width:' + descWidthIn.toFixed(4) + 'in;font-size:9pt;">' + safeText + '</span>';
    } else if (p.fieldKey && p.fieldKey.match(/line\d+units$/)) {
      html += '<span class="field" style="left:' + leftIn.toFixed(4) + 'in;top:' + topIn.toFixed(4) + 'in;font-size:9pt;">' + safeText + '</span>';
    } else {
      html += '<span class="field" style="left:' + leftIn.toFixed(4) + 'in;top:' + topIn.toFixed(4) + 'in;">' + safeText + '</span>';
    }
  }
  html += '</div>';
  html += '</body></html>';
  res.type('html').send(html);
});

// Alignment test print - prints a grid pattern to calibrate field positions
app.get('/api/print/alignment-test', function(req, res) {
  // Re-load data from disk to ensure we have the latest
  try {
    var freshData = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    Object.keys(freshData).forEach(function(k) { data[k] = freshData[k]; });
  } catch(e) { console.error('Alignment test: failed to re-read data:', e.message); }
  colShift = (typeof data.colShift === 'number') ? data.colShift : 0;
  rowShift = (typeof data.rowShift === 'number') ? data.rowShift : 0;
  console.log('Alignment Test Print: using colShift=' + colShift + ', rowShift=' + rowShift);
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
  // Re-load data from disk to ensure we have the latest
  try {
    var freshData = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    Object.keys(freshData).forEach(function(k) { data[k] = freshData[k]; });
  } catch(e) { console.error('ESC/P2 print: failed to re-read data:', e.message); }
  colShift = (typeof data.colShift === 'number') ? data.colShift : 0;
  rowShift = (typeof data.rowShift === 'number') ? data.rowShift : 0;
  customAlignment = data.customAlignment || null;
  customAlignment22a = data.customAlignment22a || null;
  console.log('ESC/P2 Print: using colShift=' + colShift + ', rowShift=' + rowShift);

  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).send('Manifest not found');

  // Auto-file manifest as printed
  fileManifestAsPrinted(manifest.id);

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
    var actualRow = Math.round(row + rowShift);
    var actualCol = Math.round(col + colShift);
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

  // Build Box 14 - always regenerate from current waste lines at print time
  var sh3 = manifest.specialHandling3 || '';
  var parts14 = [];
  for (var b14 = 1; b14 <= wasteLineCount; b14++) {
    var pid14 = manifest['waste' + b14 + 'ProfileId'] || '';
    var ctype14 = manifest['waste' + b14 + 'ContainerType'] || '';
    var csize14 = manifest['waste' + b14 + 'ContainerSize'] || '';
    var desc14 = manifest['waste' + b14 + 'Description'] || '';
    if (!desc14 && !pid14) continue;
    var entry = '';
    // Strip waste stream name from profileId - keep only the numeric ID portion
    var cleanPid14 = pid14 ? pid14.split(/\s+/)[0] : '';
    if (cleanPid14) entry += cleanPid14;
    if (csize14) entry += (entry ? ' ' : '') + csize14;
    if (ctype14) entry += (entry ? ' ' : '') + ctype14;
    if (entry) parts14.push('9b.' + b14 + '= ' + entry);
  }
  var autoText14 = parts14.join(', ');
  var sh1 = '';
  var sh2 = '';
  if (autoText14.length > 75) {
    var cut14b = autoText14.lastIndexOf(', ', 75);
    if (cut14b <= 0) cut14b = 75;
    sh1 = autoText14.substring(0, cut14b);
    var autoRest14 = autoText14.substring(cut14b).replace(/^,?\s*/, '');
    if (autoRest14.length > 75) {
      var cut14c = autoRest14.lastIndexOf(', ', 75);
      if (cut14c <= 0) cut14c = 75;
      sh2 = autoRest14.substring(0, cut14c);
      var overflow14 = autoRest14.substring(cut14c).replace(/^,?\s*/, '');
      if (overflow14 && !sh3) sh3 = overflow14;
    } else {
      sh2 = autoRest14;
    }
  } else {
    sh1 = autoText14;
  }

  // ===== PAGE 1: Main Form (8700-22) =====
  var page1 = createCanvas();

  // Box 1 - Generator EPA ID
  placeText(page1, MAP.generatorEpaId.row, MAP.generatorEpaId.col, manifest.generatorEpaId);
  // Box 2 - Page ("Page 1 of" is preprinted on the form, only print total pages)
  if (manifest.pageTotal) placeText(page1, MAP.totalPages.row, MAP.totalPages.col, manifest.pageTotal);
  // Box 4 - Manifest Tracking Number
  if (manifest.manifestTrackingNum) placeText(page1, MAP.manifestTrackingNum.row, MAP.manifestTrackingNum.col, manifest.manifestTrackingNum);
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
    var contMap = getActiveForm22aMap();
    var remainingLines = wasteLineCount - 4;
    var contPageNum = 2;
    var manifestLineStart = 5;
    var contPageCount = Math.ceil(remainingLines / CONT_MAX_WASTE_LINES);
    // Swap to 22A shifts for continuation pages
    var savedMainColShift = colShift;
    var savedMainRowShift = rowShift;
    colShift = (typeof data.colShift22a === 'number') ? data.colShift22a : 0;
    rowShift = (typeof data.rowShift22a === 'number') ? data.rowShift22a : 0;

    for (var cpIdx = 0; cpIdx < contPageCount; cpIdx++) {
      var linesOnThisPage = Math.min(remainingLines, CONT_MAX_WASTE_LINES);
      var contPage = createCanvas();

      // Box 21 - Generator EPA ID
      placeText(contPage, contMap.generatorEpaId.row, contMap.generatorEpaId.col, manifest.generatorEpaId);
      // Box 22 - Page
      // Page number not printed - "Page __" is preprinted; only print total pages ("of __")
      placeText(contPage, contMap.totalPages.row, contMap.totalPages.col, String(totalPages));
      // Box 23 - Manifest Tracking Number
      placeText(contPage, contMap.manifestTrackingNum.row, contMap.manifestTrackingNum.col, manifest.manifestTrackingNum);
      // Box 24 - Generator Name
      placeText(contPage, contMap.generatorName.row, contMap.generatorName.col, manifest.generatorName);
      // Box 25 - Transporter
      placeText(contPage, contMap.contTransporterName.row, contMap.contTransporterName.col, manifest.contTransporterName);
      placeText(contPage, contMap.contTransporterEpaId.row, contMap.contTransporterEpaId.col, manifest.contTransporterEpaId);
      // Box 26 - Transporter 2
      placeText(contPage, contMap.contTransporter2Name.row, contMap.contTransporter2Name.col, manifest.contTransporter2Name);
      placeText(contPage, contMap.contTransporter2EpaId.row, contMap.contTransporter2EpaId.col, manifest.contTransporter2EpaId);

      // Box 27-31 - Waste lines on this continuation page (with rowOffset support)
      for (var cw = 0; cw < linesOnThisPage; cw++) {
        var mLineNum = manifestLineStart + cw;
        var contRow = CONT_WASTE_START_ROW + (cw * CONT_WASTE_ROW_SPACING);
        var cwW = mLineNum;
        var cwWasteDesc = manifest['waste' + cwW + 'Description'] || '';
        var cwDescRow1Width = contMap.wasteContainerNum.col - contMap.wasteDesc.col - 1;
        var cwDescLns = wrapDescLines(cwWasteDesc, cwDescRow1Width, 55);
        for (var cwDl = 0; cwDl < cwDescLns.length && cwDl < 2; cwDl++) {
          placeText(contPage, contRow + cwDl + (contMap.wasteDesc.rowOffset || 0), contMap.wasteDesc.col, cwDescLns[cwDl]);
        }
        placeText(contPage, contRow + (contMap.wasteHm.rowOffset || 0), contMap.wasteHm.col, manifest['waste' + cwW + 'HM']);
        placeText(contPage, contRow + (contMap.wasteContainerNum.rowOffset || 0), contMap.wasteContainerNum.col, manifest['waste' + cwW + 'ContainerNum']);
        placeText(contPage, contRow + (contMap.wasteContainerType.rowOffset || 0), contMap.wasteContainerType.col, manifest['waste' + cwW + 'ContainerType']);
        placeText(contPage, contRow + (contMap.wasteQty.rowOffset || 0), contMap.wasteQty.col, manifest['waste' + cwW + 'Qty']);
        placeText(contPage, contRow + (contMap.wasteUom.rowOffset || 0), contMap.wasteUom.col, manifest['waste' + cwW + 'Unit']);
        var cwCodeArr = parseWasteCodes((manifest['waste' + cwW + 'WasteCodes'] || '').trim());
        for (var cwCi = 0; cwCi < 6 && cwCi < cwCodeArr.length; cwCi++) {
          var cwWcRow = contRow + (cwCi >= 3 ? 1 : 0) + (contMap.wasteWc1.rowOffset || 0);
          var cwWcColOff = (cwCi % 3) * 5;
          placeText(contPage, cwWcRow, contMap.wasteWc1.col + cwWcColOff, cwCodeArr[cwCi]);
        }
      }

      // Box 32 - Special Handling (build from THIS page's waste lines only)
      var contParts14b = [];
      var contSh3b = manifest.specialHandling3 || '';
      for (var cp14b = 0; cp14b < linesOnThisPage; cp14b++) {
        var cpLN = manifestLineStart + cp14b;
        var cpPidB = manifest['waste' + cpLN + 'ProfileId'] || '';
        var cpCleanB = cpPidB ? cpPidB.split(/\s+/)[0] : '';
        var cpSzB = manifest['waste' + cpLN + 'ContainerSize'] || '';
        var cpTpB = manifest['waste' + cpLN + 'ContainerType'] || '';
        var cpDsB = manifest['waste' + cpLN + 'Description'] || '';
        if (!cpDsB && !cpPidB) continue;
        var cpEnt = '9b.' + cpLN + '= ';
        if (cpCleanB) cpEnt += cpCleanB;
        if (cpSzB) cpEnt += ' ' + cpSzB;
        if (cpTpB) cpEnt += ' ' + cpTpB;
        contParts14b.push(cpEnt.trim());
      }
      var contAutoB = contParts14b.join(', ');
      var contSh1 = '', contSh2 = '';
      if (contAutoB.length > 75) {
        var cCutB = contAutoB.lastIndexOf(', ', 75);
        if (cCutB <= 0) cCutB = 75;
        contSh1 = contAutoB.substring(0, cCutB);
        var cRestB = contAutoB.substring(cCutB).replace(/^,?\s*/, '');
        if (cRestB.length > 75) {
          var cCut2B = cRestB.lastIndexOf(', ', 75);
          if (cCut2B <= 0) cCut2B = 75;
          contSh2 = cRestB.substring(0, cCut2B);
          var cOverB = cRestB.substring(cCut2B).replace(/^,?\s*/, '');
          if (cOverB && !contSh3b) contSh3b = cOverB;
        } else {
          contSh2 = cRestB;
        }
      } else {
        contSh1 = contAutoB;
      }
      placeText(contPage, contMap.specialHandling.row, contMap.specialHandling.col, contSh1);
      placeText(contPage, contMap.specialHandling2.row, contMap.specialHandling2.col, contSh2);
      placeText(contPage, contMap.specialHandling3.row, contMap.specialHandling3.col, contSh3b);
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
    // Restore main form shifts
    colShift = savedMainColShift;
    rowShift = savedMainRowShift;
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

  // Wrap in HTML with bold pre for darker printing
  var htmlOutput = '<!DOCTYPE html><html><head><title>Print Manifest</title><style>';
  htmlOutput += '@media print { @page { margin: 0; } body { margin: 0; } }';
  htmlOutput += 'body { margin: 0; padding: 0; }';
  htmlOutput += 'pre { font-family: "Courier New", Courier, monospace; font-size: 10pt; font-weight: bold; margin: 0; padding: 0; line-height: 1; }';
  htmlOutput += '</style></head><body><pre>' + output.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') + '</pre>';
  htmlOutput += '<script>window.print();</script></body></html>';
  res.set('Content-Type', 'text/html');
  res.send(htmlOutput);
});

// ESC/P2 raw print - generates .prn file for direct Epson LQ-590II printing
// Bypasses browser entirely - no margins, precise positioning
// Apply alignment overrides to RAW_MAP by computing deltas from FORM_8700_MAP defaults
function getActiveRawMap() {
  var base = getBaseRawMap();
  if (!customAlignment) return base;
  var baseForm = getBaseMap();
  var merged = {};
  var keys = Object.keys(base);
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    if (customAlignment[key] && baseForm[key]) {
      var deltaRow = customAlignment[key].row - baseForm[key].row;
      var deltaCol = customAlignment[key].col - baseForm[key].col;
      merged[key] = { row: base[key].row + deltaRow, col: base[key].col + deltaCol };
    } else {
      merged[key] = base[key];
    }
  }
  return merged;
}

var RAW_MAP = {
  // Original 12 CPI positions for direct printer output (no browser margin)
  generatorEpaId:     { row: 4, col: 18 },
  manifestTrackingNum: { row: 2, col: 55 },
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
  waste1hm:           { row: 20, col: 3 },
  waste2hm:           { row: 23, col: 3 },
  waste3hm:           { row: 26, col: 3 },
  waste4hm:           { row: 29, col: 3 },
  waste1desc:         { row: 20, col: 7 },
  waste2desc:         { row: 23, col: 7 },
  waste3desc:         { row: 26, col: 7 },
  waste4desc:         { row: 29, col: 7 },
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
  // Re-load data from disk to ensure we have the latest
  try {
    var freshData = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    Object.keys(freshData).forEach(function(k) { data[k] = freshData[k]; });
  } catch(e) { console.error('ESC/P2 raw print: failed to re-read data:', e.message); }
  customAlignment = data.customAlignment || null;
  customAlignment22a = data.customAlignment22a || null;

  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).send('Manifest not found');

  // Auto-file manifest as printed
  fileManifestAsPrinted(manifest.id);

  var M = getActiveRawMap();
  var commands = [];

  function addBytes(arr) { commands.push(Buffer.from(arr)); }
  function addText(text) { commands.push(Buffer.from(text, 'ascii')); }

  // Current shift values for ESC/P2 positioning (swapped for continuation pages)
  var prnColShift = colShift;
  var prnRowShift = rowShift;

  // Position print head and print text
  // Row/col are 1-based. Horizontal: (col-1)*5 in 1/60" units. Vertical: (row-1)*60 in 1/360" units.
  // Applies prnColShift/prnRowShift (can be fractional, e.g. 0.5)
  function printAt(row, col, text) {
    if (!text) return;
    text = String(text);
    // ESC ( V 2 0 mL mH - absolute vertical position in 1/360"
    var vPos = Math.round((row - 1 + prnRowShift) * 60);
    if (vPos < 0) vPos = 0;
    var mL = vPos & 0xFF;
    var mH = (vPos >> 8) & 0xFF;
    addBytes([0x1B, 0x28, 0x56, 0x02, 0x00, mL, mH]);
    // ESC $ nL nH - absolute horizontal position in 1/60"
    var hPos = Math.round((col - 1 + prnColShift) * 5);
    if (hPos < 0) hPos = 0;
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
  addBytes([0x1B, 0x45]); // ESC E - Bold/emphasized on
  addBytes([0x1B, 0x47]); // ESC G - Double-strike on

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
  // Box 2 - Page ("Page 1 of" is preprinted, only print total pages)
  if (manifest.pageTotal) printAt(M.totalPages.row, M.totalPages.col, manifest.pageTotal);
  // Box 4 - Manifest Tracking Number
  if (manifest.manifestTrackingNum) printAt(M.manifestTrackingNum.row, M.manifestTrackingNum.col, manifest.manifestTrackingNum);
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

  // Box 14 - Special Handling (always regenerate from waste lines at print time)
  var sh3 = manifest.specialHandling3 || '';
  var parts14 = [];
  for (var b14 = 1; b14 <= wasteLineCount; b14++) {
    var pid14 = manifest['waste' + b14 + 'ProfileId'] || '';
    var csize14 = manifest['waste' + b14 + 'ContainerSize'] || '';
    var ctype14 = manifest['waste' + b14 + 'ContainerType'] || '';
    var desc14 = manifest['waste' + b14 + 'Description'] || '';
    if (!desc14 && !pid14) continue;
    var label14 = '9b.' + b14 + '= ';
    // Strip waste stream name from profileId - keep only the numeric ID portion
    var cleanPid = pid14 ? pid14.split(/\s+/)[0] : '';
    if (cleanPid) label14 += cleanPid;
    if (csize14) label14 += ' ' + csize14;
    if (ctype14) label14 += ' ' + ctype14;
    parts14.push(label14.trim());
  }
  var autoText = parts14.join(', ');
  var sh1 = '';
  var sh2 = '';
  if (autoText.length > 75) {
    var cut14 = autoText.lastIndexOf(', ', 75);
    if (cut14 <= 0) cut14 = 75;
    sh1 = autoText.substring(0, cut14);
    var rest14 = autoText.substring(cut14).replace(/^,?\s*/, '');
    if (rest14.length > 75) {
      var cut14b = rest14.lastIndexOf(', ', 75);
      if (cut14b <= 0) cut14b = 75;
      sh2 = rest14.substring(0, cut14b);
      var overflow14 = rest14.substring(cut14b).replace(/^,?\s*/, '');
      if (overflow14 && !sh3) sh3 = overflow14;
    } else {
      sh2 = rest14;
    }
  } else {
    sh1 = autoText;
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
    var contMap = getActiveForm22aMap();
    var remainingLines = wasteLineCount - 4;
    var contPageNum = 2;
    var manifestLineStart = 5;
    var contPageCount = Math.ceil(remainingLines / CONT_MAX_WASTE_LINES);
    // Swap to 22A shifts for continuation pages
    prnColShift = (typeof data.colShift22a === 'number') ? data.colShift22a : 0;
    prnRowShift = (typeof data.rowShift22a === 'number') ? data.rowShift22a : 0;

    for (var cpIdx = 0; cpIdx < contPageCount; cpIdx++) {
      var linesOnThisPage = Math.min(remainingLines, CONT_MAX_WASTE_LINES);

      // Re-initialize for new page
      addBytes([0x1B, 0x40]); // ESC @ reset
      addBytes([0x1B, 0x4D]); // 12 CPI
      addBytes([0x1B, 0x32]); // 6 LPI
      addBytes([0x1B, 0x45]); // ESC E - Bold/emphasized on
      addBytes([0x1B, 0x47]); // ESC G - Double-strike on

      printAt(contMap.generatorEpaId.row, contMap.generatorEpaId.col, manifest.generatorEpaId);
      // Page number not printed - "Page __" is preprinted; only print total pages ("of __")
      printAt(contMap.totalPages.row, contMap.totalPages.col, String(totalPages));
      printAt(contMap.manifestTrackingNum.row, contMap.manifestTrackingNum.col, manifest.manifestTrackingNum);
      printAt(contMap.generatorName.row, contMap.generatorName.col, manifest.generatorName);
      printAt(contMap.contTransporterName.row, contMap.contTransporterName.col, manifest.contTransporterName);
      printAt(contMap.contTransporterEpaId.row, contMap.contTransporterEpaId.col, manifest.contTransporterEpaId);
      printAt(contMap.contTransporter2Name.row, contMap.contTransporter2Name.col, manifest.contTransporter2Name);
      printAt(contMap.contTransporter2EpaId.row, contMap.contTransporter2EpaId.col, manifest.contTransporter2EpaId);

      // Waste lines on continuation page
      for (var cw = 0; cw < linesOnThisPage; cw++) {
        var mLineNum = manifestLineStart + cw;
        var contRow = CONT_WASTE_START_ROW + (cw * CONT_WASTE_ROW_SPACING);
        var cwDesc = manifest['waste' + mLineNum + 'Description'] || '';
        var cwDescLines = wrapDesc(cwDesc, 40, 40);
        for (var cdl = 0; cdl < cwDescLines.length && cdl < 2; cdl++) {
          printAt(contRow + cdl + (contMap.wasteDesc.rowOffset || 0), contMap.wasteDesc.col, cwDescLines[cdl]);
        }
        printAt(contRow + (contMap.wasteHm.rowOffset || 0), contMap.wasteHm.col, manifest['waste' + mLineNum + 'HM']);
        printAt(contRow + (contMap.wasteContainerNum.rowOffset || 0), contMap.wasteContainerNum.col, manifest['waste' + mLineNum + 'ContainerNum']);
        printAt(contRow + (contMap.wasteContainerType.rowOffset || 0), contMap.wasteContainerType.col, manifest['waste' + mLineNum + 'ContainerType']);
        printAt(contRow + (contMap.wasteQty.rowOffset || 0), contMap.wasteQty.col, manifest['waste' + mLineNum + 'Qty']);
        printAt(contRow + (contMap.wasteUom.rowOffset || 0), contMap.wasteUom.col, manifest['waste' + mLineNum + 'Unit']);
        var cwCodes = parseWC((manifest['waste' + mLineNum + 'WasteCodes'] || '').trim());
        for (var cci = 0; cci < 6 && cci < cwCodes.length; cci++) {
          var cwRow = contRow + (cci >= 3 ? 1 : 0) + (contMap.wasteWc1.rowOffset || 0);
          var cwColOff = (cci % 3) * 5;
          printAt(cwRow, contMap.wasteWc1.col + cwColOff, cwCodes[cci]);
        }
      }

      // Box 32 - Special Handling (build from THIS page's waste lines only)
      var contParts14c = [];
      var contSh3c = manifest.specialHandling3 || '';
      for (var cp14c = 0; cp14c < linesOnThisPage; cp14c++) {
        var cpLNc = manifestLineStart + cp14c;
        var cpPidC = manifest['waste' + cpLNc + 'ProfileId'] || '';
        var cpCleanC = cpPidC ? cpPidC.split(/\s+/)[0] : '';
        var cpSzC = manifest['waste' + cpLNc + 'ContainerSize'] || '';
        var cpTpC = manifest['waste' + cpLNc + 'ContainerType'] || '';
        var cpDsC = manifest['waste' + cpLNc + 'Description'] || '';
        if (!cpDsC && !cpPidC) continue;
        var cpEntC = '9b.' + cpLNc + '= ';
        if (cpCleanC) cpEntC += cpCleanC;
        if (cpSzC) cpEntC += ' ' + cpSzC;
        if (cpTpC) cpEntC += ' ' + cpTpC;
        contParts14c.push(cpEntC.trim());
      }
      var contAutoC = contParts14c.join(', ');
      var cSh1 = '', cSh2 = '';
      if (contAutoC.length > 75) {
        var cCutC = contAutoC.lastIndexOf(', ', 75);
        if (cCutC <= 0) cCutC = 75;
        cSh1 = contAutoC.substring(0, cCutC);
        var cRestC = contAutoC.substring(cCutC).replace(/^,?\s*/, '');
        if (cRestC.length > 75) {
          var cCut2C = cRestC.lastIndexOf(', ', 75);
          if (cCut2C <= 0) cCut2C = 75;
          cSh2 = cRestC.substring(0, cCut2C);
          var cOverC = cRestC.substring(cCut2C).replace(/^,?\s*/, '');
          if (cOverC && !contSh3c) contSh3c = cOverC;
        } else {
          cSh2 = cRestC;
        }
      } else {
        cSh1 = contAutoC;
      }
      printAt(contMap.specialHandling.row, contMap.specialHandling.col, cSh1);
      printAt(contMap.specialHandling2.row, contMap.specialHandling2.col, cSh2);
      printAt(contMap.specialHandling3.row, contMap.specialHandling3.col, contSh3c);
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
  // Re-sync alignment from data object before printing
  // Re-load data from disk to ensure we have the latest
  try {
    var freshData = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    Object.keys(freshData).forEach(function(k) { data[k] = freshData[k]; });
  } catch(e) { console.error('Direct print: failed to re-read data:', e.message); }
  colShift = (typeof data.colShift === 'number') ? data.colShift : 0;
  rowShift = (typeof data.rowShift === 'number') ? data.rowShift : 0;
  customAlignment = data.customAlignment || null;
  customAlignment22a = data.customAlignment22a || null;
  console.log('Direct Print: using colShift=' + colShift + ', rowShift=' + rowShift + ', hasCustomAlignment=' + !!customAlignment);

  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).send('Manifest not found');

  // Auto-file manifest as printed
  fileManifestAsPrinted(manifest.id);

  console.log('Direct Print manifest id=' + manifest.id + ', manifestTrackingNum="' + (manifest.manifestTrackingNum || '') + '"');

  var M = getActiveRawMap();
  console.log('Direct Print M.manifestTrackingNum=' + JSON.stringify(M.manifestTrackingNum));

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
    console.log('  Direct print waste' + wlc + ': desc="' + (manifest['waste' + wlc + 'Description'] || '') + '" pid="' + (manifest['waste' + wlc + 'ProfileId'] || '') + '" csize="' + (manifest['waste' + wlc + 'ContainerSize'] || '') + '" ctype="' + (manifest['waste' + wlc + 'ContainerType'] || '') + '"');
    if (wd || wco || wq) wasteLineCount = wlc;
  }
  if (wasteLineCount < 4) wasteLineCount = 4;
  console.log('Direct print: rawWLC=' + rawWLC + ', activeWasteLines=' + wasteLineCount);
  var totalPages = wasteLineCount <= 4 ? 1 : Math.ceil((wasteLineCount - 4) / CONT_MAX_WASTE_LINES) + 1;

  // === Page 1 - Main Form (8700-22) ===
  placeAt(M.generatorEpaId.row, M.generatorEpaId.col, manifest.generatorEpaId);
  // Box 2 - "Page 1 of" is preprinted, only print total pages
  if (manifest.pageTotal) placeAt(M.totalPages.row, M.totalPages.col, manifest.pageTotal);
  // Box 4 - Manifest Tracking Number
  if (manifest.manifestTrackingNum) placeAt(M.manifestTrackingNum.row, M.manifestTrackingNum.col, manifest.manifestTrackingNum);
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
    // Non RCRA always gets ERG # 171
    if (/non[\s\-]*rcra/i.test(text)) return '171';
    // Look up UN/NA number (with or without UN/NA prefix)
    var match = text.match(/(?:UN|NA)\s*(\d{4})/i);
    if (match && ERG_LOOKUP[match[1]]) {
      return ERG_LOOKUP[match[1]];
    }
    // Fallback: try to find a bare 4-digit number that matches the lookup
    var bareMatch = text.match(/\b(\d{4})\b/g);
    if (bareMatch) {
      for (var bi = 0; bi < bareMatch.length; bi++) {
        if (ERG_LOOKUP[bareMatch[bi]]) return ERG_LOOKUP[bareMatch[bi]];
      }
    }
    return '';
  }

  // Format shipping description: Title Case with exceptions
  // n.o.s. = lowercase, PG/UN/NA/RQ = uppercase, everything else = Title Case
  function formatShipDesc(text) {
    if (!text) return '';
    // Collapse all extra whitespace first
    var result = text.replace(/\s+/g, ' ').trim();
    // Clean up spacing around commas: remove space before comma, ensure single space after
    result = result.replace(/\s*,\s*/g, ', ');
    // First lowercase everything, then title-case each word
    result = result.toLowerCase().replace(/\b\w/g, function(c) { return c.toUpperCase(); });
    // Fix exceptions: n.o.s. should be all lowercase
    result = result.replace(/N\.O\.S\./gi, 'n.o.s.');
    result = result.replace(/\bNos\b/gi, 'n.o.s.');
    // PG should be uppercase
    result = result.replace(/\bPg\b/g, 'PG');
    // Fix packing group Roman numerals: Iii -> III, Ii -> II (after PG or standalone)
    result = result.replace(/\bIii\b/g, 'III');
    result = result.replace(/\bIi\b/g, 'II');
    // UN and NA (hazmat ID prefixes) should be uppercase
    result = result.replace(/\bUn(\d)/g, 'UN$1');
    result = result.replace(/\bNa(\d)/g, 'NA$1');
    // RQ should be uppercase
    result = result.replace(/\bRq,/g, 'RQ,');
    result = result.replace(/\bRq\b/g, 'RQ');
    // RCRA should be uppercase
    result = result.replace(/\bRcra\b/g, 'RCRA');
    // ERG should be uppercase (in case it gets title-cased)
    result = result.replace(/\bErg\b/g, 'ERG');
    return result;
  }

  // Waste Lines 1-4
  var maxOnPage1 = Math.min(wasteLineCount, 4);
  for (var w = 1; w <= maxOnPage1; w++) {
    var hmKey = 'waste' + w + 'hm';
    var descKey = 'waste' + w + 'desc';
    var baseRow = M[hmKey].row;
    placeAt(M[hmKey].row, M[hmKey].col, manifest['waste' + w + 'HM']);
    var rawDesc = (manifest['waste' + w + 'Description'] || '').replace(/\s+/g, ' ').trim();
    var ergNum = getErgNumber(rawDesc);
    var descText = formatShipDesc(rawDesc);
    if (ergNum && descText.indexOf('ERG') === -1) descText += ', ERG # ' + ergNum;
    var descMaxWidth = M['waste' + w + 'containerNum'].col - M[descKey].col - 1;
    var descLines = wrapDesc(descText, descMaxWidth, descMaxWidth);
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

  // Box 14 - Special Handling (always regenerate from waste lines at print time)
  var sh3 = manifest.specialHandling3 || '';
  // Always build profile/container info from current waste lines
  var parts14 = [];
  for (var b14 = 1; b14 <= wasteLineCount; b14++) {
    var pid14 = manifest['waste' + b14 + 'ProfileId'] || '';
    var csize14 = manifest['waste' + b14 + 'ContainerSize'] || '';
    var ctype14 = manifest['waste' + b14 + 'ContainerType'] || '';
    var desc14 = manifest['waste' + b14 + 'Description'] || '';
    if (!desc14 && !pid14) continue;
    var label14 = '9b.' + b14 + '= ';
    // Strip waste stream name from profileId - keep only the numeric ID portion
    var cleanPid = pid14 ? pid14.split(/\s+/)[0] : '';
    if (cleanPid) label14 += cleanPid;
    if (csize14) label14 += ' ' + csize14;
    if (ctype14) label14 += ' ' + ctype14;
    parts14.push(label14.trim());
  }
  var autoText = parts14.join(', ');
  console.log('Direct print Box14: parts14=' + JSON.stringify(parts14) + ', autoText="' + autoText + '" (' + autoText.length + ' chars)');
  var sh1 = '';
  var sh2 = '';
  if (autoText.length > 75) {
    var cut14 = autoText.lastIndexOf(', ', 75);
    if (cut14 <= 0) cut14 = 75;
    sh1 = autoText.substring(0, cut14);
    var rest14 = autoText.substring(cut14).replace(/^,?\s*/, '');
    if (rest14.length > 75) {
      var cut14b = rest14.lastIndexOf(', ', 75);
      if (cut14b <= 0) cut14b = 75;
      sh2 = rest14.substring(0, cut14b);
      var overflow14 = rest14.substring(cut14b).replace(/^,?\s*/, '');
      if (overflow14 && !sh3) sh3 = overflow14;
    } else {
      sh2 = rest14;
    }
  } else {
    sh1 = autoText;
  }
  placeAt(M.specialHandling.row, M.specialHandling.col, sh1);
  placeAt(M.specialHandling2.row, M.specialHandling2.col, sh2);
  placeAt(M.specialHandling3.row, M.specialHandling3.col, sh3);
  placeAt(M.generatorCertName.row, M.generatorCertName.col, manifest.generatorPrintName);

  // === Continuation Pages ===
  if (wasteLineCount > 4) {
    var contMap = getActiveRaw22aMap();
    var remainingLines = wasteLineCount - 4;
    var contPageNum = 2;
    var manifestLineStart = 5;
    var contPageCount = Math.ceil(remainingLines / CONT_MAX_WASTE_LINES);
    for (var cpIdx = 0; cpIdx < contPageCount; cpIdx++) {
      var pg = cpIdx + 2;
      var linesOnThisPage = Math.min(remainingLines, CONT_MAX_WASTE_LINES);
      placeAt(contMap.generatorEpaId.row, contMap.generatorEpaId.col, manifest.generatorEpaId, pg);
      // Page number not printed - "Page __" is preprinted; only print total pages ("of __")
      placeAt(contMap.totalPages.row, contMap.totalPages.col, String(totalPages), pg);
      placeAt(contMap.manifestTrackingNum.row, contMap.manifestTrackingNum.col, manifest.manifestTrackingNum, pg);
      placeAt(contMap.generatorName.row, contMap.generatorName.col, manifest.generatorName, pg);
      placeAt(contMap.contTransporterName.row, contMap.contTransporterName.col, manifest.contTransporterName, pg);
      placeAt(contMap.contTransporterEpaId.row, contMap.contTransporterEpaId.col, manifest.contTransporterEpaId, pg);
      placeAt(contMap.contTransporter2Name.row, contMap.contTransporter2Name.col, manifest.contTransporter2Name, pg);
      placeAt(contMap.contTransporter2EpaId.row, contMap.contTransporter2EpaId.col, manifest.contTransporter2EpaId, pg);
      for (var cw = 0; cw < linesOnThisPage; cw++) {
        var mLineNum = manifestLineStart + cw;
        var contRow = CONT_WASTE_START_ROW + (cw * CONT_WASTE_ROW_SPACING);
        var cwRawDesc = (manifest['waste' + mLineNum + 'Description'] || '').replace(/\s+/g, ' ').trim();
        var cwErgNum = getErgNumber(cwRawDesc);
        var cwDesc = formatShipDesc(cwRawDesc);
        if (cwErgNum && cwDesc.indexOf('ERG') === -1) cwDesc += ', ERG # ' + cwErgNum;
        var cwDescLines = wrapDesc(cwDesc, 40, 40);
        for (var cdl = 0; cdl < cwDescLines.length && cdl < 2; cdl++) {
          placeAt(contRow + cdl + (contMap.wasteDesc.rowOffset || 0), contMap.wasteDesc.col, cwDescLines[cdl], pg);
        }
        placeAt(contRow + (contMap.wasteHm.rowOffset || 0), contMap.wasteHm.col, manifest['waste' + mLineNum + 'HM'], pg);
        placeAt(contRow + (contMap.wasteContainerNum.rowOffset || 0), contMap.wasteContainerNum.col, manifest['waste' + mLineNum + 'ContainerNum'], pg);
        placeAt(contRow + (contMap.wasteContainerType.rowOffset || 0), contMap.wasteContainerType.col, manifest['waste' + mLineNum + 'ContainerType'], pg);
        placeAt(contRow + (contMap.wasteQty.rowOffset || 0), contMap.wasteQty.col, manifest['waste' + mLineNum + 'Qty'], pg);
        placeAt(contRow + (contMap.wasteUom.rowOffset || 0), contMap.wasteUom.col, manifest['waste' + mLineNum + 'Unit'], pg);
        var cwCodes = parseWC((manifest['waste' + mLineNum + 'WasteCodes'] || '').trim());
        for (var cci = 0; cci < 6 && cci < cwCodes.length; cci++) {
          var cwRow2 = contRow + (cci >= 3 ? 1 : 0) + (contMap.wasteWc1.rowOffset || 0);
          var cwColOff = (cci % 3) * 5;
          placeAt(cwRow2, contMap.wasteWc1.col + cwColOff, cwCodes[cci], pg);
        }
      }
      // Box 14 - build from THIS page's waste lines only (not main page lines 1-4)
      var contParts14 = [];
      var contSh3 = manifest.specialHandling3 || '';
      for (var cp14 = 0; cp14 < linesOnThisPage; cp14++) {
        var cpLineNum = manifestLineStart + cp14;
        var cpPid = manifest['waste' + cpLineNum + 'ProfileId'] || '';
        var cpCleanPid = cpPid ? cpPid.split(/\s+/)[0] : '';
        var cpSize = manifest['waste' + cpLineNum + 'ContainerSize'] || '';
        var cpType = manifest['waste' + cpLineNum + 'ContainerType'] || '';
        var cpDesc = manifest['waste' + cpLineNum + 'Description'] || '';
        if (!cpDesc && !cpPid) continue;
        var cpEntry = '9b.' + cpLineNum + '= ';
        if (cpCleanPid) cpEntry += cpCleanPid;
        if (cpSize) cpEntry += ' ' + cpSize;
        if (cpType) cpEntry += ' ' + cpType;
        contParts14.push(cpEntry.trim());
      }
      var contAutoText = contParts14.join(', ');
      var cSh1 = '', cSh2 = '';
      if (contAutoText.length > 75) {
        var cCut = contAutoText.lastIndexOf(', ', 75);
        if (cCut <= 0) cCut = 75;
        cSh1 = contAutoText.substring(0, cCut);
        var cRest = contAutoText.substring(cCut).replace(/^,?\s*/, '');
        if (cRest.length > 75) {
          var cCut2 = cRest.lastIndexOf(', ', 75);
          if (cCut2 <= 0) cCut2 = 75;
          cSh2 = cRest.substring(0, cCut2);
          var cOverflow = cRest.substring(cCut2).replace(/^,?\s*/, '');
          if (cOverflow && !contSh3) contSh3 = cOverflow;
        } else {
          cSh2 = cRest;
        }
      } else {
        cSh1 = contAutoText;
      }
      placeAt(contMap.specialHandling.row, contMap.specialHandling.col, cSh1, pg);
      placeAt(contMap.specialHandling2.row, contMap.specialHandling2.col, cSh2, pg);
      placeAt(contMap.specialHandling3.row, contMap.specialHandling3.col, contSh3, pg);
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
  // Apply saved alignment shifts (colShift/rowShift from Alignment tab) + query param overrides
  var savedColShiftIn = colShift / CPI;  // convert column shift to inches
  var savedRowShiftIn = rowShift / LPI;  // convert row shift to inches
  var colOffsetIn = BASE_LEFT_OFFSET + savedColShiftIn + (parseFloat(req.query.colOffset) || 0);
  var rowOffsetIn = BASE_TOP_OFFSET + savedRowShiftIn + (parseFloat(req.query.rowOffset) || 0);
  // Continuation page (22A) shifts - applied to page 2+
  var colShift22aVal = (typeof data.colShift22a === 'number') ? data.colShift22a : 0;
  var rowShift22aVal = (typeof data.rowShift22a === 'number') ? data.rowShift22a : 0;
  var contColOffsetIn = BASE_LEFT_OFFSET + (colShift22aVal / CPI) + (parseFloat(req.query.colOffset) || 0);
  var contRowOffsetIn = BASE_TOP_OFFSET + (rowShift22aVal / LPI) + (parseFloat(req.query.rowOffset) || 0);

  var html = '<!DOCTYPE html><html><head><title>Print Manifest</title><style>';
  html += '@page { margin: 0; size: 8.5in 11in; }';
  html += '@media print { body { margin: 0; padding: 0; } .no-print { display: none !important; } }';
  html += 'body { margin: 0; padding: 0; }';
  html += '.page { position: relative; width: 8.5in; height: 11in; overflow: hidden; page-break-after: always; }';
  html += '.page:last-child { page-break-after: auto; }';
  html += '.field { position: absolute; font-family: "Courier New", Courier, monospace; font-size: 9pt; font-weight: bold; line-height: 1; white-space: pre; margin: 0; padding: 0; }';
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

  // Support ?page=N to print only a specific page
  var requestedPage = parseInt(req.query.page) || 0;

  for (var pg = 1; pg <= maxPage; pg++) {
    // Skip pages that weren't requested (if a specific page was requested)
    if (requestedPage > 0 && pg !== requestedPage) continue;
    html += '<div class="page">';
    // Use continuation shifts for page 2+, main form shifts for page 1
    var pgColOff = (pg >= 2) ? contColOffsetIn : colOffsetIn;
    var pgRowOff = (pg >= 2) ? contRowOffsetIn : rowOffsetIn;
    for (var fi = 0; fi < placements.length; fi++) {
      var p = placements[fi];
      if (p.page !== pg) continue;
      // Convert row/col to inches: col 1 = 0in from left, row 1 = 0in from top
      var leftIn = ((p.col - 1) / CPI) + pgColOff;
      var topIn = ((p.row - 1) / LPI) + pgRowOff;
      // Escape HTML
      var safeText = p.text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      html += '<span class="field" style="left:' + leftIn.toFixed(4) + 'in;top:' + topIn.toFixed(4) + 'in;">' + safeText + '</span>';
    }
    html += '</div>';
  }

  html += '</body></html>';
  res.type('html').send(html);
});

// === Non-Haz Manifest Print Endpoints ===
// Same form layout as hazardous, but uses non-haz alignment data

// Non-Haz ESC/P2 text print (same as /api/print/manifest/:id but with non-haz alignment)
app.get('/api/print/nonhaz/:id', function(req, res) {
  // Re-load data from disk to ensure we have the latest
  try {
    var freshData = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    Object.keys(freshData).forEach(function(k) { data[k] = freshData[k]; });
  } catch(e) { console.error('Non-Haz ESC/P2 print: failed to re-read data:', e.message); }
  // Re-sync non-haz alignment variables from data
  customAlignmentNonhaz = data.customAlignmentNonhaz || null;
  savedDefaultsNonhaz = data.savedDefaultsNonhaz || null;
  // Use non-haz alignment shifts
  var nhColShift = (typeof data.colShiftNonhaz === 'number') ? data.colShiftNonhaz : 0;
  var nhRowShift = (typeof data.rowShiftNonhaz === 'number') ? data.rowShiftNonhaz : 0;
  // Temporarily set global colShift/rowShift to non-haz values
  var origColShift = colShift;
  var origRowShift = rowShift;
  var origCustomAlignment = customAlignment;
  var origCustomAlignment22a = customAlignment22a;
  colShift = nhColShift;
  rowShift = nhRowShift;
  customAlignment = data.customAlignmentNonhaz || null;
  customAlignment22a = data.customAlignmentNonhaz22a || null;
  console.log('Non-Haz ESC/P2 Print: using colShift=' + nhColShift + ', rowShift=' + nhRowShift);

  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) {
    colShift = origColShift; rowShift = origRowShift;
    customAlignment = origCustomAlignment; customAlignment22a = origCustomAlignment22a;
    return res.status(404).send('Manifest not found');
  }

  // Auto-file manifest as printed
  fileManifestAsPrinted(manifest.id);

  var MAP = getActiveMap();

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

  function placeText(pageLines, row, col, text) {
    if (!text) return;
    text = String(text);
    var actualRow = Math.round(row + rowShift);
    var actualCol = Math.round(col + colShift);
    if (actualRow < 0 || actualRow >= CANVAS_ROWS) return;
    if (actualCol < 0) actualCol = 0;
    var line = pageLines[actualRow];
    for (var ci = 0; ci < text.length; ci++) {
      var idx = actualCol + ci;
      if (idx >= 0 && idx < line.length) {
        line = line.substring(0, idx) + text[ci] + line.substring(idx + 1);
      }
    }
    pageLines[actualRow] = line;
  }

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

  // Build page 1
  var page1 = createCanvas();
  placeText(page1, MAP.generatorEpaId.row, MAP.generatorEpaId.col, manifest.generatorEpaId);
  if (manifest.pageTotal) placeText(page1, MAP.totalPages.row, MAP.totalPages.col, manifest.pageTotal);
  // Box 4 - Manifest Tracking Number
  if (manifest.manifestTrackingNum) placeText(page1, MAP.manifestTrackingNum.row, MAP.manifestTrackingNum.col, manifest.manifestTrackingNum);
  placeText(page1, MAP.emergencyPhone.row, MAP.emergencyPhone.col, manifest.emergencyPhone);
  placeText(page1, MAP.generatorName.row, MAP.generatorName.col, manifest.generatorName);
  placeText(page1, MAP.generatorPhone.row, MAP.generatorPhone.col, manifest.generatorPhone);
  placeText(page1, MAP.generatorMailAddr.row, MAP.generatorMailAddr.col, manifest.generatorAddress);
  placeText(page1, MAP.generatorMailCity.row, MAP.generatorMailCity.col, manifest.generatorCityStZip);
  placeText(page1, MAP.generatorSiteAddr.row, MAP.generatorSiteAddr.col, manifest.genSiteAddress);
  placeText(page1, MAP.generatorSiteCity.row, MAP.generatorSiteCity.col, manifest.genSiteCityStZip);
  placeText(page1, MAP.transporter1Name.row, MAP.transporter1Name.col, manifest.transporter1Name);
  placeText(page1, MAP.transporter1EpaId.row, MAP.transporter1EpaId.col, manifest.transporter1EpaId);
  placeText(page1, MAP.transporter2Name.row, MAP.transporter2Name.col, manifest.transporter2Name);
  placeText(page1, MAP.transporter2EpaId.row, MAP.transporter2EpaId.col, manifest.transporter2EpaId);
  placeText(page1, MAP.facilityName.row, MAP.facilityName.col, manifest.facilityName);
  placeText(page1, MAP.facilityEpaId.row, MAP.facilityEpaId.col, manifest.facilityEpaId);
  placeText(page1, MAP.facilityAddress.row, MAP.facilityAddress.col, manifest.facilityAddress);
  placeText(page1, MAP.facilityPhone.row, MAP.facilityPhone.col, manifest.facilityPhone);
  placeText(page1, MAP.facilityCity.row, MAP.facilityCity.col, manifest.facilityCityStZip);

  // ERG lookup
  var ERG_LOOKUP_NH = {
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

  function getErgNumberNH(text) {
    if (!text) return '';
    if (/non[\s\-]*rcra/i.test(text)) return '171';
    var match = text.match(/(?:UN|NA)\s*(\d{4})/i);
    if (match && ERG_LOOKUP_NH[match[1]]) return ERG_LOOKUP_NH[match[1]];
    var bareMatch = text.match(/\b(\d{4})\b/g);
    if (bareMatch) {
      for (var bi = 0; bi < bareMatch.length; bi++) {
        if (ERG_LOOKUP_NH[bareMatch[bi]]) return ERG_LOOKUP_NH[bareMatch[bi]];
      }
    }
    return '';
  }

  function formatShipDescNH(text) {
    if (!text) return '';
    var result = text.replace(/\s+/g, ' ').trim();
    result = result.replace(/\s*,\s*/g, ', ');
    result = result.toLowerCase().replace(/\b\w/g, function(c) { return c.toUpperCase(); });
    result = result.replace(/N\.O\.S\./gi, 'n.o.s.');
    result = result.replace(/\bNos\b/gi, 'n.o.s.');
    result = result.replace(/\bPg\b/g, 'PG');
    result = result.replace(/\bIii\b/g, 'III');
    result = result.replace(/\bIi\b/g, 'II');
    result = result.replace(/\bUn(\d)/g, 'UN$1');
    result = result.replace(/\bNa(\d)/g, 'NA$1');
    result = result.replace(/\bRq,/g, 'RQ,');
    result = result.replace(/\bRq\b/g, 'RQ');
    result = result.replace(/\bRcra\b/g, 'RCRA');
    result = result.replace(/\bErg\b/g, 'ERG');
    return result;
  }

  // Waste lines 1-4
  var maxOnPage1 = Math.min(wasteLineCount, 4);
  for (var w = 1; w <= maxOnPage1; w++) {
    var hmKey = 'waste' + w + 'hm';
    var descKey = 'waste' + w + 'desc';
    placeText(page1, MAP[hmKey].row, MAP[hmKey].col, manifest['waste' + w + 'HM']);
    var rawDesc = (manifest['waste' + w + 'Description'] || '').replace(/\s+/g, ' ').trim();
    var ergNum = getErgNumberNH(rawDesc);
    var descText = formatShipDescNH(rawDesc);
    if (ergNum && descText.indexOf('ERG') === -1) descText += ', ERG # ' + ergNum;
    var descMaxWidth = MAP['waste' + w + 'containerNum'].col - MAP[descKey].col - 1;
    var descLines = wrapDesc(descText, descMaxWidth, descMaxWidth);
    for (var dl = 0; dl < descLines.length && dl < 2; dl++) {
      placeText(page1, MAP[hmKey].row + dl, MAP[descKey].col, descLines[dl]);
    }
    placeText(page1, MAP[hmKey].row, MAP['waste' + w + 'containerNum'].col, manifest['waste' + w + 'ContainerNum']);
    placeText(page1, MAP[hmKey].row, MAP['waste' + w + 'container'].col, manifest['waste' + w + 'ContainerType']);
    placeText(page1, MAP[hmKey].row, MAP['waste' + w + 'qty'].col, manifest['waste' + w + 'Qty']);
    placeText(page1, MAP[hmKey].row, MAP['waste' + w + 'uom'].col, manifest['waste' + w + 'Unit']);
    var wcKey = 'waste' + w + 'wc';
    var codes = parseWC((manifest['waste' + w + 'WasteCodes'] || '').trim());
    for (var ci = 0; ci < 6 && ci < codes.length; ci++) {
      var wcField = wcKey + (ci + 1);
      if (MAP[wcField]) placeText(page1, MAP[wcField].row, MAP[wcField].col, codes[ci]);
    }
  }

  // Box 14 - Special Handling
  var sh3nh = manifest.specialHandling3 || '';
  var parts14nh = [];
  for (var b14 = 1; b14 <= wasteLineCount; b14++) {
    var pid14 = manifest['waste' + b14 + 'ProfileId'] || '';
    var csize14 = manifest['waste' + b14 + 'ContainerSize'] || '';
    var ctype14 = manifest['waste' + b14 + 'ContainerType'] || '';
    var desc14 = manifest['waste' + b14 + 'Description'] || '';
    if (!desc14 && !pid14) continue;
    var label14 = '9b.' + b14 + '= ';
    var cleanPid = pid14 ? pid14.split(/\s+/)[0] : '';
    if (cleanPid) label14 += cleanPid;
    if (csize14) label14 += ' ' + csize14;
    if (ctype14) label14 += ' ' + ctype14;
    parts14nh.push(label14.trim());
  }
  var autoTextNh = parts14nh.join(', ');
  var sh1nh = '';
  var sh2nh = '';
  if (autoTextNh.length > 75) {
    var cut14 = autoTextNh.lastIndexOf(', ', 75);
    if (cut14 <= 0) cut14 = 75;
    sh1nh = autoTextNh.substring(0, cut14);
    var rest14 = autoTextNh.substring(cut14).replace(/^,?\s*/, '');
    if (rest14.length > 75) {
      var cut14b = rest14.lastIndexOf(', ', 75);
      if (cut14b <= 0) cut14b = 75;
      sh2nh = rest14.substring(0, cut14b);
      var overflow14 = rest14.substring(cut14b).replace(/^,?\s*/, '');
      if (overflow14 && !sh3nh) sh3nh = overflow14;
    } else {
      sh2nh = rest14;
    }
  } else {
    sh1nh = autoTextNh;
  }
  placeText(page1, MAP.specialHandling.row, MAP.specialHandling.col, sh1nh);
  placeText(page1, MAP.specialHandling2.row, MAP.specialHandling2.col, sh2nh);
  placeText(page1, MAP.specialHandling3.row, MAP.specialHandling3.col, sh3nh);
  placeText(page1, MAP.generatorCertName.row, MAP.generatorCertName.col, manifest.generatorPrintName);

  var pages = [page1];

  // Continuation pages
  if (wasteLineCount > 4) {
    var savedMainColShiftNh = colShift;
    var savedMainRowShiftNh = rowShift;
    colShift = (typeof data.colShift22a === 'number') ? data.colShift22a : 0;
    rowShift = (typeof data.rowShift22a === 'number') ? data.rowShift22a : 0;
    var contMap22a = getActive22aMap();
    var remainingLines = wasteLineCount - 4;
    var manifestLineStart = 5;
    while (remainingLines > 0) {
      var linesOnPage = Math.min(remainingLines, CONT_MAX_WASTE_LINES);
      var contPage = createCanvas();
      placeText(contPage, contMap22a.generatorEpaId.row, contMap22a.generatorEpaId.col, manifest.generatorEpaId);
      placeText(contPage, contMap22a.totalPages.row, contMap22a.totalPages.col, String(totalPages));
      placeText(contPage, contMap22a.manifestTrackingNum.row, contMap22a.manifestTrackingNum.col, manifest.manifestTrackingNum);
      placeText(contPage, contMap22a.generatorName.row, contMap22a.generatorName.col, manifest.generatorName);
      placeText(contPage, contMap22a.contTransporterName.row, contMap22a.contTransporterName.col, manifest.contTransporterName);
      placeText(contPage, contMap22a.contTransporterEpaId.row, contMap22a.contTransporterEpaId.col, manifest.contTransporterEpaId);
      placeText(contPage, contMap22a.contTransporter2Name.row, contMap22a.contTransporter2Name.col, manifest.contTransporter2Name);
      placeText(contPage, contMap22a.contTransporter2EpaId.row, contMap22a.contTransporter2EpaId.col, manifest.contTransporter2EpaId);
      for (var cw = 0; cw < linesOnPage; cw++) {
        var mLineNum = manifestLineStart + cw;
        var contRow = CONT_WASTE_START_ROW + (cw * CONT_WASTE_ROW_SPACING);
        var cwRawDesc = (manifest['waste' + mLineNum + 'Description'] || '').replace(/\s+/g, ' ').trim();
        var cwErgNum = getErgNumberNH(cwRawDesc);
        var cwDesc = formatShipDescNH(cwRawDesc);
        if (cwErgNum && cwDesc.indexOf('ERG') === -1) cwDesc += ', ERG # ' + cwErgNum;
        var cwDescLines = wrapDesc(cwDesc, 40, 40);
        for (var cdl = 0; cdl < cwDescLines.length && cdl < 2; cdl++) {
          placeText(contPage, contRow + cdl, contMap22a.wasteDesc.col, cwDescLines[cdl]);
        }
        placeText(contPage, contRow, contMap22a.wasteHm.col, manifest['waste' + mLineNum + 'HM']);
        placeText(contPage, contRow, contMap22a.wasteContainerNum.col, manifest['waste' + mLineNum + 'ContainerNum']);
        placeText(contPage, contRow, contMap22a.wasteContainerType.col, manifest['waste' + mLineNum + 'ContainerType']);
        placeText(contPage, contRow, contMap22a.wasteQty.col, manifest['waste' + mLineNum + 'Qty']);
        placeText(contPage, contRow, contMap22a.wasteUom.col, manifest['waste' + mLineNum + 'Unit']);
        var cwCodes = parseWC((manifest['waste' + mLineNum + 'WasteCodes'] || '').trim());
        for (var cci = 0; cci < 6 && cci < cwCodes.length; cci++) {
          var cwRow2 = contRow + (cci >= 3 ? 1 : 0);
          var cwColOff = (cci % 3) * 5;
          placeText(contPage, cwRow2, contMap22a.wasteWc1.col + cwColOff, cwCodes[cci]);
        }
      }
      // Box 14 for continuation
      var contParts = [];
      for (var cp14 = 0; cp14 < linesOnPage; cp14++) {
        var cpLineNum = manifestLineStart + cp14;
        var cpPid = manifest['waste' + cpLineNum + 'ProfileId'] || '';
        var cpCleanPid = cpPid ? cpPid.split(/\s+/)[0] : '';
        var cpSize = manifest['waste' + cpLineNum + 'ContainerSize'] || '';
        var cpType = manifest['waste' + cpLineNum + 'ContainerType'] || '';
        var cpDesc = manifest['waste' + cpLineNum + 'Description'] || '';
        if (!cpDesc && !cpPid) continue;
        var cpEntry = '9b.' + cpLineNum + '= ';
        if (cpCleanPid) cpEntry += cpCleanPid;
        if (cpSize) cpEntry += ' ' + cpSize;
        if (cpType) cpEntry += ' ' + cpType;
        contParts.push(cpEntry.trim());
      }
      var cAutoText = contParts.join(', ');
      if (cAutoText.length <= 75) {
        placeText(contPage, contMap22a.specialHandling.row, contMap22a.specialHandling.col, cAutoText);
      } else {
        var cCut = cAutoText.lastIndexOf(', ', 75);
        if (cCut <= 0) cCut = 75;
        placeText(contPage, contMap22a.specialHandling.row, contMap22a.specialHandling.col, cAutoText.substring(0, cCut));
        var cRest = cAutoText.substring(cCut).replace(/^,?\s*/, '');
        if (contMap22a.specialHandling2) placeText(contPage, contMap22a.specialHandling2.row, contMap22a.specialHandling2.col, cRest.substring(0, 75));
      }
      placeText(contPage, contMap22a.contTransporterPrintName.row, contMap22a.contTransporterPrintName.col, manifest.contTransporterPrintName);
      placeText(contPage, contMap22a.contTransporterDate.row, contMap22a.contTransporterDate.col, manifest.contTransporterDate);
      placeText(contPage, contMap22a.contTransporter2PrintName.row, contMap22a.contTransporter2PrintName.col, manifest.contTransporter2PrintName);
      placeText(contPage, contMap22a.contTransporter2Date.row, contMap22a.contTransporter2Date.col, manifest.contTransporter2Date);
      placeText(contPage, contMap22a.contDiscrepancyInfo.row, contMap22a.contDiscrepancyInfo.col, manifest.contDiscrepancyInfo);
      pages.push(contPage);
      remainingLines -= linesOnPage;
      manifestLineStart += linesOnPage;
    }
    colShift = savedMainColShiftNh;
    rowShift = savedMainRowShiftNh;
  }

  // Build output text
  var output = '';
  for (var pg = 0; pg < pages.length; pg++) {
    if (pg > 0) output += '\f';
    for (var rl = 0; rl < pages[pg].length; rl++) {
      output += pages[pg][rl].replace(/\s+$/, '') + '\n';
    }
  }

  // Restore original alignment
  colShift = origColShift;
  rowShift = origRowShift;
  customAlignment = origCustomAlignment;
  customAlignment22a = origCustomAlignment22a;

  // Wrap in HTML with bold pre for darker printing
  var htmlOutput = '<!DOCTYPE html><html><head><title>Print Non-Haz Manifest</title><style>';
  htmlOutput += '@media print { @page { margin: 0; } body { margin: 0; } }';
  htmlOutput += 'body { margin: 0; padding: 0; }';
  htmlOutput += 'pre { font-family: "Courier New", Courier, monospace; font-size: 10pt; font-weight: bold; margin: 0; padding: 0; line-height: 1; }';
  htmlOutput += '</style></head><body><pre>' + output.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') + '</pre>';
  htmlOutput += '<script>window.print();</script></body></html>';
  res.type('text/html').send(htmlOutput);
});

// Non-Haz HTML CSS-positioned print (same as /api/print/direct/:id but with non-haz alignment)
app.get('/api/print/nonhaz-direct/:id', function(req, res) {
  // Re-load data from disk to ensure we have the latest
  try {
    var freshData = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    Object.keys(freshData).forEach(function(k) { data[k] = freshData[k]; });
  } catch(e) { console.error('Non-Haz Direct print: failed to re-read data:', e.message); }
  // Re-sync non-haz alignment variables from data
  customAlignmentNonhaz = data.customAlignmentNonhaz || null;
  savedDefaultsNonhaz = data.savedDefaultsNonhaz || null;
  // Use non-haz alignment shifts
  var nhColShift = (typeof data.colShiftNonhaz === 'number') ? data.colShiftNonhaz : 0;
  var nhRowShift = (typeof data.rowShiftNonhaz === 'number') ? data.rowShiftNonhaz : 0;
  console.log('Non-Haz Direct Print: using colShift=' + nhColShift + ', rowShift=' + nhRowShift + ', hasCustomAlignment=' + !!customAlignmentNonhaz);

  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).send('Manifest not found');

  // Auto-file manifest as printed
  fileManifestAsPrinted(manifest.id);

  // Use non-haz raw map (same positions as haz, but separate alignment)
  var nhCustomAlignmentRaw = data.customAlignmentNonhaz || null;
  var M = getActiveNonhazRawMap();

  var placements = [];
  function placeAt(row, col, text, pg) {
    if (!text) return;
    placements.push({ row: row, col: col, text: String(text), page: pg || 1 });
  }

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

  // Page 1 fields
  placeAt(M.generatorEpaId.row, M.generatorEpaId.col, manifest.generatorEpaId);
  if (manifest.pageTotal) placeAt(M.totalPages.row, M.totalPages.col, manifest.pageTotal);
  // Box 4 - Manifest Tracking Number
  if (manifest.manifestTrackingNum) placeAt(M.manifestTrackingNum.row, M.manifestTrackingNum.col, manifest.manifestTrackingNum);
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

  // ERG lookup
  var ERG_LOOKUP_NH2 = {
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

  function getErgNumberNH2(text) {
    if (!text) return '';
    if (/non[\s\-]*rcra/i.test(text)) return '171';
    var match = text.match(/(?:UN|NA)\s*(\d{4})/i);
    if (match && ERG_LOOKUP_NH2[match[1]]) return ERG_LOOKUP_NH2[match[1]];
    var bareMatch = text.match(/\b(\d{4})\b/g);
    if (bareMatch) {
      for (var bi = 0; bi < bareMatch.length; bi++) {
        if (ERG_LOOKUP_NH2[bareMatch[bi]]) return ERG_LOOKUP_NH2[bareMatch[bi]];
      }
    }
    return '';
  }

  function formatShipDescNH2(text) {
    if (!text) return '';
    var result = text.replace(/\s+/g, ' ').trim();
    result = result.replace(/\s*,\s*/g, ', ');
    result = result.toLowerCase().replace(/\b\w/g, function(c) { return c.toUpperCase(); });
    result = result.replace(/N\.O\.S\./gi, 'n.o.s.');
    result = result.replace(/\bNos\b/gi, 'n.o.s.');
    result = result.replace(/\bPg\b/g, 'PG');
    result = result.replace(/\bIii\b/g, 'III');
    result = result.replace(/\bIi\b/g, 'II');
    result = result.replace(/\bUn(\d)/g, 'UN$1');
    result = result.replace(/\bNa(\d)/g, 'NA$1');
    result = result.replace(/\bRq,/g, 'RQ,');
    result = result.replace(/\bRq\b/g, 'RQ');
    result = result.replace(/\bRcra\b/g, 'RCRA');
    result = result.replace(/\bErg\b/g, 'ERG');
    return result;
  }

  // Waste Lines 1-4
  var maxOnPage1 = Math.min(wasteLineCount, 4);
  for (var w = 1; w <= maxOnPage1; w++) {
    var hmKey = 'waste' + w + 'hm';
    var descKey = 'waste' + w + 'desc';
    var baseRow = M[hmKey].row;
    placeAt(M[hmKey].row, M[hmKey].col, manifest['waste' + w + 'HM']);
    var rawDesc = (manifest['waste' + w + 'Description'] || '').replace(/\s+/g, ' ').trim();
    var ergNum = getErgNumberNH2(rawDesc);
    var descText = formatShipDescNH2(rawDesc);
    if (ergNum && descText.indexOf('ERG') === -1) descText += ', ERG # ' + ergNum;
    var descMaxWidth = M['waste' + w + 'containerNum'].col - M[descKey].col - 1;
    var descLines = wrapDesc(descText, descMaxWidth, descMaxWidth);
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
      if (M[wcField]) placeAt(M[wcField].row, M[wcField].col, codes[ci]);
    }
  }

  // Box 14
  var sh3nh2 = manifest.specialHandling3 || '';
  var parts14nh2 = [];
  for (var b14 = 1; b14 <= wasteLineCount; b14++) {
    var pid14 = manifest['waste' + b14 + 'ProfileId'] || '';
    var csize14 = manifest['waste' + b14 + 'ContainerSize'] || '';
    var ctype14 = manifest['waste' + b14 + 'ContainerType'] || '';
    var desc14 = manifest['waste' + b14 + 'Description'] || '';
    if (!desc14 && !pid14) continue;
    var label14 = '9b.' + b14 + '= ';
    var cleanPid = pid14 ? pid14.split(/\s+/)[0] : '';
    if (cleanPid) label14 += cleanPid;
    if (csize14) label14 += ' ' + csize14;
    if (ctype14) label14 += ' ' + ctype14;
    parts14nh2.push(label14.trim());
  }
  var autoTextNh2 = parts14nh2.join(', ');
  var sh1nh2 = '';
  var sh2nh2 = '';
  if (autoTextNh2.length > 75) {
    var cut14 = autoTextNh2.lastIndexOf(', ', 75);
    if (cut14 <= 0) cut14 = 75;
    sh1nh2 = autoTextNh2.substring(0, cut14);
    var rest14 = autoTextNh2.substring(cut14).replace(/^,?\s*/, '');
    if (rest14.length > 75) {
      var cut14b = rest14.lastIndexOf(', ', 75);
      if (cut14b <= 0) cut14b = 75;
      sh2nh2 = rest14.substring(0, cut14b);
      var overflow14 = rest14.substring(cut14b).replace(/^,?\s*/, '');
      if (overflow14 && !sh3nh2) sh3nh2 = overflow14;
    } else {
      sh2nh2 = rest14;
    }
  } else {
    sh1nh2 = autoTextNh2;
  }
  placeAt(M.specialHandling.row, M.specialHandling.col, sh1nh2);
  placeAt(M.specialHandling2.row, M.specialHandling2.col, sh2nh2);
  placeAt(M.specialHandling3.row, M.specialHandling3.col, sh3nh2);
  placeAt(M.generatorCertName.row, M.generatorCertName.col, manifest.generatorPrintName);

  // Continuation Pages
  if (wasteLineCount > 4) {
    var contMap = getActiveNonhazRaw22aMap();
    var remainingLines = wasteLineCount - 4;
    var contPageNum = 2;
    var manifestLineStart = 5;
    var contPageCount = Math.ceil(remainingLines / CONT_MAX_WASTE_LINES);
    for (var cpIdx = 0; cpIdx < contPageCount; cpIdx++) {
      var pg = cpIdx + 2;
      var linesOnThisPage = Math.min(remainingLines, CONT_MAX_WASTE_LINES);
      placeAt(contMap.generatorEpaId.row, contMap.generatorEpaId.col, manifest.generatorEpaId, pg);
      placeAt(contMap.totalPages.row, contMap.totalPages.col, String(totalPages), pg);
      placeAt(contMap.manifestTrackingNum.row, contMap.manifestTrackingNum.col, manifest.manifestTrackingNum, pg);
      placeAt(contMap.generatorName.row, contMap.generatorName.col, manifest.generatorName, pg);
      placeAt(contMap.contTransporterName.row, contMap.contTransporterName.col, manifest.contTransporterName, pg);
      placeAt(contMap.contTransporterEpaId.row, contMap.contTransporterEpaId.col, manifest.contTransporterEpaId, pg);
      placeAt(contMap.contTransporter2Name.row, contMap.contTransporter2Name.col, manifest.contTransporter2Name, pg);
      placeAt(contMap.contTransporter2EpaId.row, contMap.contTransporter2EpaId.col, manifest.contTransporter2EpaId, pg);
      for (var cw = 0; cw < linesOnThisPage; cw++) {
        var mLineNum = manifestLineStart + cw;
        var contRow = CONT_WASTE_START_ROW + (cw * CONT_WASTE_ROW_SPACING);
        var cwRawDesc = (manifest['waste' + mLineNum + 'Description'] || '').replace(/\s+/g, ' ').trim();
        var cwErgNum = getErgNumberNH2(cwRawDesc);
        var cwDesc = formatShipDescNH2(cwRawDesc);
        if (cwErgNum && cwDesc.indexOf('ERG') === -1) cwDesc += ', ERG # ' + cwErgNum;
        var cwDescLines = wrapDesc(cwDesc, 40, 40);
        for (var cdl = 0; cdl < cwDescLines.length && cdl < 2; cdl++) {
          placeAt(contRow + cdl + (contMap.wasteDesc.rowOffset || 0), contMap.wasteDesc.col, cwDescLines[cdl], pg);
        }
        placeAt(contRow + (contMap.wasteHm.rowOffset || 0), contMap.wasteHm.col, manifest['waste' + mLineNum + 'HM'], pg);
        placeAt(contRow + (contMap.wasteContainerNum.rowOffset || 0), contMap.wasteContainerNum.col, manifest['waste' + mLineNum + 'ContainerNum'], pg);
        placeAt(contRow + (contMap.wasteContainerType.rowOffset || 0), contMap.wasteContainerType.col, manifest['waste' + mLineNum + 'ContainerType'], pg);
        placeAt(contRow + (contMap.wasteQty.rowOffset || 0), contMap.wasteQty.col, manifest['waste' + mLineNum + 'Qty'], pg);
        placeAt(contRow + (contMap.wasteUom.rowOffset || 0), contMap.wasteUom.col, manifest['waste' + mLineNum + 'Unit'], pg);
        var cwCodes = parseWC((manifest['waste' + mLineNum + 'WasteCodes'] || '').trim());
        for (var cci = 0; cci < 6 && cci < cwCodes.length; cci++) {
          var cwRow2 = contRow + (cci >= 3 ? 1 : 0) + (contMap.wasteWc1.rowOffset || 0);
          var cwColOff = (cci % 3) * 5;
          placeAt(cwRow2, contMap.wasteWc1.col + cwColOff, cwCodes[cci], pg);
        }
      }
      // Box 14 continuation
      var contParts14 = [];
      var contSh3 = manifest.specialHandling3 || '';
      for (var cp14 = 0; cp14 < linesOnThisPage; cp14++) {
        var cpLineNum = manifestLineStart + cp14;
        var cpPid = manifest['waste' + cpLineNum + 'ProfileId'] || '';
        var cpCleanPid = cpPid ? cpPid.split(/\s+/)[0] : '';
        var cpSize = manifest['waste' + cpLineNum + 'ContainerSize'] || '';
        var cpType = manifest['waste' + cpLineNum + 'ContainerType'] || '';
        var cpDesc = manifest['waste' + cpLineNum + 'Description'] || '';
        if (!cpDesc && !cpPid) continue;
        var cpEntry = '9b.' + cpLineNum + '= ';
        if (cpCleanPid) cpEntry += cpCleanPid;
        if (cpSize) cpEntry += ' ' + cpSize;
        if (cpType) cpEntry += ' ' + cpType;
        contParts14.push(cpEntry.trim());
      }
      var contAutoText = contParts14.join(', ');
      var cSh1 = '', cSh2 = '';
      if (contAutoText.length > 75) {
        var cCut = contAutoText.lastIndexOf(', ', 75);
        if (cCut <= 0) cCut = 75;
        cSh1 = contAutoText.substring(0, cCut);
        var cRest = contAutoText.substring(cCut).replace(/^,?\s*/, '');
        if (cRest.length > 75) {
          var cCut2 = cRest.lastIndexOf(', ', 75);
          if (cCut2 <= 0) cCut2 = 75;
          cSh2 = cRest.substring(0, cCut2);
          var cOverflow = cRest.substring(cCut2).replace(/^,?\s*/, '');
          if (cOverflow && !contSh3) contSh3 = cOverflow;
        } else {
          cSh2 = cRest;
        }
      } else {
        cSh1 = contAutoText;
      }
      placeAt(contMap.specialHandling.row, contMap.specialHandling.col, cSh1, pg);
      placeAt(contMap.specialHandling2.row, contMap.specialHandling2.col, cSh2, pg);
      placeAt(contMap.specialHandling3.row, contMap.specialHandling3.col, contSh3, pg);
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

  // Build HTML
  var CPI = 12;
  var LPI = 6;
  var BASE_TOP_OFFSET = 0.5;
  var BASE_LEFT_OFFSET = 0.0;
  var savedColShiftIn = nhColShift / CPI;
  var savedRowShiftIn = nhRowShift / LPI;
  var colOffsetIn = BASE_LEFT_OFFSET + savedColShiftIn + (parseFloat(req.query.colOffset) || 0);
  var rowOffsetIn = BASE_TOP_OFFSET + savedRowShiftIn + (parseFloat(req.query.rowOffset) || 0);
  // Non-haz continuation uses same 22A shifts (shared with haz for now)
  var nhColShift22a = (typeof data.colShift22a === 'number') ? data.colShift22a : 0;
  var nhRowShift22a = (typeof data.rowShift22a === 'number') ? data.rowShift22a : 0;
  var contColOffsetIn = BASE_LEFT_OFFSET + (nhColShift22a / CPI) + (parseFloat(req.query.colOffset) || 0);
  var contRowOffsetIn = BASE_TOP_OFFSET + (nhRowShift22a / LPI) + (parseFloat(req.query.rowOffset) || 0);

  var html = '<!DOCTYPE html><html><head><title>Print Non-Haz Manifest</title><style>';
  html += '@page { margin: 0; size: 8.5in 11in; }';
  html += '@media print { body { margin: 0; padding: 0; } .no-print { display: none !important; } }';
  html += 'body { margin: 0; padding: 0; }';
  html += '.page { position: relative; width: 8.5in; height: 11in; overflow: hidden; page-break-after: always; }';
  html += '.page:last-child { page-break-after: auto; }';
  html += '.field { position: absolute; font-family: "Courier New", Courier, monospace; font-size: 9pt; font-weight: bold; line-height: 1; white-space: pre; margin: 0; padding: 0; }';
  html += '.toolbar { padding: 10px; background: #f0f0f0; text-align: center; font-family: sans-serif; }';
  html += '.toolbar button { padding: 8px 20px; font-size: 16px; margin: 0 5px; cursor: pointer; }';
  html += '.toolbar .print-btn { background: #059669; color: white; border: none; border-radius: 4px; }';
  html += '.toolbar .close-btn { background: #6b7280; color: white; border: none; border-radius: 4px; }';
  html += '.toolbar label { margin: 0 8px; font-size: 13px; }';
  html += '.toolbar input[type=number] { width: 60px; padding: 2px 4px; }';
  html += '</style></head><body>';

  html += '<div class="no-print toolbar">';
  html += '<button class="print-btn" onclick="window.print()">Print Non-Haz Manifest</button>';
  html += '<button class="close-btn" onclick="window.close()">Close</button>';
  html += '<span style="margin-left:20px;font-size:12px;color:#666">Select your Epson LQ-590II in the print dialog. Set margins to None.</span>';
  html += '</div>';

  var maxPage = 1;
  for (var pi = 0; pi < placements.length; pi++) {
    if (placements[pi].page > maxPage) maxPage = placements[pi].page;
  }

  var requestedPage = parseInt(req.query.page) || 0;

  for (var pg = 1; pg <= maxPage; pg++) {
    if (requestedPage > 0 && pg !== requestedPage) continue;
    html += '<div class="page">';
    var pgColOff = (pg >= 2) ? contColOffsetIn : colOffsetIn;
    var pgRowOff = (pg >= 2) ? contRowOffsetIn : rowOffsetIn;
    for (var fi = 0; fi < placements.length; fi++) {
      var p = placements[fi];
      if (p.page !== pg) continue;
      var leftIn = ((p.col - 1) / CPI) + pgColOff;
      var topIn = ((p.row - 1) / LPI) + pgRowOff;
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

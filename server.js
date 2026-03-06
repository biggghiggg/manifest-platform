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

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Initialize data
var defaultData = {
  generators: [],
  transporters: [],
  facilities: [],
  wasteStreams: [],
  manifests: []
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
      if (unMatch) unNum = unMatch[1].trim();

      // Hazard Class
      var hcMatch = fullText.match(/Hazard Class[:\s]+(\S+)/i);
      if (hcMatch) hazardClass = hcMatch[1].trim();

      // Packing Group - try multiple patterns (I, II, III or PGI, PGII, PGIII)
      var pgMatch = fullText.match(/Packing Group[:\s]+(I{1,3}V?|PG\s*I{1,3}V?)/i);
      if (!pgMatch) pgMatch = fullText.match(/Packing Group[:\s]+(\S+)/i);
      if (pgMatch) packingGroup = pgMatch[1].trim();

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
  // Box 1 - Generator's US EPA ID Number
  generatorEpaId:     { row: 4, col: 18 },
  // Box 2 - Page __ of __
  page:               { row: 4, col: 38 },
  totalPages:         { row: 4, col: 42 },
  // Box 3 - Emergency Response Phone
  emergencyPhone:     { row: 4, col: 47 },
  // Box 5 - Generator
  generatorName:      { row: 6, col: 5 },
  // Mailing Address (LEFT side of Box 5)
  generatorMailAddr:  { row: 7, col: 5 },
  generatorMailCity:  { row: 8, col: 5 },
  generatorPhone:     { row: 8, col: 28 },
  // Site Address (RIGHT side of Box 5)
  generatorSiteAddr:  { row: 7, col: 42 },
  generatorSiteCity:  { row: 8, col: 42 },
  // Box 6 - Transporter 1
  transporter1Name:   { row: 10, col: 5 },
  transporter1EpaId:  { row: 10, col: 52 },
  // Box 7 - Transporter 2
  transporter2Name:   { row: 12, col: 5 },
  transporter2EpaId:  { row: 12, col: 52 },
  // Box 8 - Designated Facility
  facilityName:       { row: 14, col: 5 },
  facilityEpaId:      { row: 14, col: 52 },
  facilityAddress:    { row: 15, col: 5 },
  facilityPhone:      { row: 16, col: 5 },
  facilityCity:       { row: 16, col: 20 },
  facilityState:      { row: 16, col: 34 },
  facilityZip:        { row: 16, col: 38 },
  // Box 9a - HM
  waste1hm:           { row: 21, col: 4 },
  waste2hm:           { row: 24, col: 4 },
  waste3hm:           { row: 27, col: 4 },
  waste4hm:           { row: 30, col: 4 },
  // Box 9b - Description
  waste1desc:         { row: 21, col: 8 },
  waste2desc:         { row: 24, col: 8 },
  waste3desc:         { row: 27, col: 8 },
  waste4desc:         { row: 30, col: 8 },
  // Box 10 - Containers (number + type)
  waste1containerNum: { row: 21, col: 48 },
  waste1container:    { row: 21, col: 52 },
  waste2containerNum: { row: 24, col: 48 },
  waste2container:    { row: 24, col: 52 },
  waste3containerNum: { row: 27, col: 48 },
  waste3container:    { row: 27, col: 52 },
  waste4containerNum: { row: 30, col: 48 },
  waste4container:    { row: 30, col: 52 },
  // Box 11 - Quantity
  waste1qty:          { row: 21, col: 57 },
  waste2qty:          { row: 24, col: 57 },
  waste3qty:          { row: 27, col: 57 },
  waste4qty:          { row: 30, col: 57 },
  // Box 12 - Unit
  waste1uom:          { row: 21, col: 60 },
  waste2uom:          { row: 24, col: 60 },
  waste3uom:          { row: 27, col: 60 },
  waste4uom:          { row: 30, col: 60 },
  // Box 13 - Waste Codes (6 per line: 3 on row 1, 3 on row 2)
  waste1wc1:          { row: 21, col: 66 },
  waste1wc2:          { row: 21, col: 71 },
  waste1wc3:          { row: 21, col: 76 },
  waste1wc4:          { row: 22, col: 66 },
  waste1wc5:          { row: 22, col: 71 },
  waste1wc6:          { row: 22, col: 76 },
  waste2wc1:          { row: 24, col: 66 },
  waste2wc2:          { row: 24, col: 71 },
  waste2wc3:          { row: 24, col: 76 },
  waste2wc4:          { row: 25, col: 66 },
  waste2wc5:          { row: 25, col: 71 },
  waste2wc6:          { row: 25, col: 76 },
  waste3wc1:          { row: 27, col: 66 },
  waste3wc2:          { row: 27, col: 71 },
  waste3wc3:          { row: 27, col: 76 },
  waste3wc4:          { row: 28, col: 66 },
  waste3wc5:          { row: 28, col: 71 },
  waste3wc6:          { row: 28, col: 76 },
  waste4wc1:          { row: 30, col: 66 },
  waste4wc2:          { row: 30, col: 71 },
  waste4wc3:          { row: 30, col: 76 },
  waste4wc4:          { row: 31, col: 66 },
  waste4wc5:          { row: 31, col: 71 },
  waste4wc6:          { row: 31, col: 76 },
  // Box 14 - Special Handling (3 lines, MIS permanent on line 3)
  specialHandling:    { row: 33, col: 5 },
  specialHandling2:   { row: 34, col: 5 },
  specialHandling3:   { row: 35, col: 5 },
  // Box 15 - Generator Certification
  generatorCertName:  { row: 38, col: 7 }
};

// Print manifest - plain text for dot matrix
// Uses manifest fields directly (as saved by the frontend)
var BUILD_VERSION = 'v12-2026-03-06';
app.get('/api/version', function(req, res) { res.json({ version: BUILD_VERSION }); });

// Alignment editor endpoints
var customAlignment = data.customAlignment || null;
var previousAlignment = data.previousAlignment || null;

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
    hasPrevious: previousAlignment !== null
  });
});

app.put('/api/alignment', function(req, res) {
  // Save current as previous before overwriting
  previousAlignment = customAlignment ? JSON.parse(JSON.stringify(customAlignment)) : null;
  data.previousAlignment = previousAlignment;
  customAlignment = req.body.map || null;
  data.customAlignment = customAlignment;
  saveData();
  res.json({ ok: true });
});

app.post('/api/alignment/reset', function(req, res) {
  // Save current as previous before resetting
  previousAlignment = customAlignment ? JSON.parse(JSON.stringify(customAlignment)) : null;
  data.previousAlignment = previousAlignment;
  customAlignment = null;
  delete data.customAlignment;
  saveData();
  res.json({ ok: true });
});

app.post('/api/alignment/undo', function(req, res) {
  if (previousAlignment === null) {
    return res.json({ ok: false, message: 'No previous settings to restore' });
  }
  // Swap current and previous
  var temp = customAlignment ? JSON.parse(JSON.stringify(customAlignment)) : null;
  customAlignment = JSON.parse(JSON.stringify(previousAlignment));
  previousAlignment = temp;
  data.customAlignment = customAlignment;
  data.previousAlignment = previousAlignment;
  saveData();
  res.json({ ok: true, map: getActiveMap() });
});

app.get('/api/print/manifest/:id', function(req, res) {
  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).send('Manifest not found');

  var MAP = getActiveMap();
  var lines = [];
  for (var l = 0; l < 66; l++) {
    var row = '';
    for (var c = 0; c < 132; c++) { row += ' '; }
    lines.push(row);
  }

  function placeText(row, col, text) {
    if (!text) return;
    text = String(text);
    if (row < 1 || row > 66) return;
    var line = lines[row - 1];
    var before = line.substring(0, col - 1);
    var after = line.substring(col - 1 + text.length);
    lines[row - 1] = before + text + after;
  }

  // Box 1 - Generator EPA ID
  placeText(MAP.generatorEpaId.row, MAP.generatorEpaId.col, manifest.generatorEpaId);
  // Box 2 - Page
  placeText(MAP.page.row, MAP.page.col, manifest.pageNum || '1');
  placeText(MAP.totalPages.row, MAP.totalPages.col, manifest.pageTotal || '1');
  // Box 3 - Emergency Response Phone
  placeText(MAP.emergencyPhone.row, MAP.emergencyPhone.col, manifest.emergencyPhone);

  // Box 5 - Generator
  placeText(MAP.generatorName.row, MAP.generatorName.col, manifest.generatorName);
  placeText(MAP.generatorPhone.row, MAP.generatorPhone.col, manifest.generatorPhone);
  placeText(MAP.generatorMailAddr.row, MAP.generatorMailAddr.col, manifest.generatorMailAddress);
  placeText(MAP.generatorMailCity.row, MAP.generatorMailCity.col, manifest.generatorMailCityStZip);
  placeText(MAP.generatorSiteAddr.row, MAP.generatorSiteAddr.col, manifest.genSiteAddress);
  placeText(MAP.generatorSiteCity.row, MAP.generatorSiteCity.col, manifest.genSiteCityStZip);

  // Box 6 - Transporter 1
  placeText(MAP.transporter1Name.row, MAP.transporter1Name.col, manifest.transporter1Name);
  placeText(MAP.transporter1EpaId.row, MAP.transporter1EpaId.col, manifest.transporter1EpaId);
  // Box 7 - Transporter 2
  placeText(MAP.transporter2Name.row, MAP.transporter2Name.col, manifest.transporter2Name);
  placeText(MAP.transporter2EpaId.row, MAP.transporter2EpaId.col, manifest.transporter2EpaId);

  // Box 8 - Facility
  placeText(MAP.facilityName.row, MAP.facilityName.col, manifest.facilityName);
  placeText(MAP.facilityPhone.row, MAP.facilityPhone.col, manifest.facilityPhone);
  placeText(MAP.facilityAddress.row, MAP.facilityAddress.col, manifest.facilityAddress);
  placeText(MAP.facilityCity.row, MAP.facilityCity.col, manifest.facilityCityStZip);
  placeText(MAP.facilityEpaId.row, MAP.facilityEpaId.col, manifest.facilityEpaId);

  // Box 9-13 - Waste lines (all positions independent)
  var descRow1Width = MAP.waste1containerNum.col - MAP.waste1desc.col - 1;
  var descContWidth = 55;
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
  for (var w = 1; w <= 4; w++) {
    var wasteDesc = manifest['waste' + w + 'Description'] || '';
    var descLines = wrapDescLines(wasteDesc, descRow1Width, descContWidth);
    var descRow = MAP['waste' + w + 'desc'].row;
    for (var dl = 0; dl < descLines.length && dl < 3; dl++) {
      placeText(descRow + dl, MAP['waste' + w + 'desc'].col, descLines[dl]);
    }
    // 9a - HM
    placeText(MAP['waste' + w + 'hm'].row, MAP['waste' + w + 'hm'].col, manifest['waste' + w + 'HM']);
    // 10 - Containers
    placeText(MAP['waste' + w + 'containerNum'].row, MAP['waste' + w + 'containerNum'].col, manifest['waste' + w + 'ContainerNum']);
    placeText(MAP['waste' + w + 'container'].row, MAP['waste' + w + 'container'].col, manifest['waste' + w + 'ContainerType']);
    // 11 - Qty
    placeText(MAP['waste' + w + 'qty'].row, MAP['waste' + w + 'qty'].col, manifest['waste' + w + 'Qty']);
    // 12 - Unit
    placeText(MAP['waste' + w + 'uom'].row, MAP['waste' + w + 'uom'].col, manifest['waste' + w + 'Unit']);
    // 13 - Waste codes (6 per line, each with independent row/col)
    var allCodes = (manifest['waste' + w + 'WasteCodes'] || '').trim();
    if (allCodes) {
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
      // Place each code at its own independent row/col from the map
      for (var ci = 0; ci < 6; ci++) {
        if (codeArr[ci]) {
          var wcKey = 'waste' + w + 'wc' + (ci + 1);
          if (MAP[wcKey]) {
            placeText(MAP[wcKey].row, MAP[wcKey].col, codeArr[ci]);
          }
        }
      }
    }
  }

  // Box 14 - Special Handling
  placeText(MAP.specialHandling.row, MAP.specialHandling.col, manifest.specialHandling);
  placeText(MAP.specialHandling2.row, MAP.specialHandling2.col, manifest.specialHandling2);
  placeText(MAP.specialHandling3.row, MAP.specialHandling3.col, manifest.specialHandling3);

  // Box 15 - Generator Certification (date NOT printed - handwritten on form)
  placeText(MAP.generatorCertName.row, MAP.generatorCertName.col, manifest.generatorPrintName);

  var output = lines.join('\n');
  res.set('Content-Type', 'text/plain');
  res.send(output);
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

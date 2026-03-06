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

      // Packing Group
      var pgMatch = fullText.match(/Packing Group[:\s]+(\S+)/i);
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
  generatorEpaId:     { row: 5, col: 27 },
  // Box 2 - Page 1 of __
  page:               { row: 5, col: 66 },
  totalPages:          { row: 5, col: 71 },
  // Box 3 - Emergency Response Phone
  emergencyPhone:     { row: 7, col: 27 },
  // Box 5 - Generator
  generatorName:      { row: 9, col: 7 },
  generatorPhone:     { row: 9, col: 52 },
  // Mailing Address (LEFT side of Box 5)
  generatorMailAddr:  { row: 10, col: 7 },
  generatorMailCity:  { row: 11, col: 7 },
  generatorMailState: { row: 11, col: 28 },
  generatorMailZip:   { row: 11, col: 33 },
  // Site Address (RIGHT side of Box 5 - same rows as mailing)
  generatorSiteAddr:  { row: 10, col: 45 },
  generatorSiteCity:  { row: 11, col: 45 },
  generatorSiteState: { row: 11, col: 64 },
  generatorSiteZip:   { row: 11, col: 68 },
  // Box 6 - Transporter 1
  transporter1Name:   { row: 15, col: 7 },
  transporter1EpaId:  { row: 15, col: 52 },
  // Box 7 - Transporter 2
  transporter2Name:   { row: 17, col: 7 },
  transporter2EpaId:  { row: 17, col: 52 },
  // Box 8 - Designated Facility
  facilityName:       { row: 19, col: 7 },
  facilityPhone:      { row: 19, col: 52 },
  facilityAddress:    { row: 20, col: 7 },
  facilityCity:       { row: 21, col: 7 },
  facilityState:      { row: 21, col: 38 },
  facilityZip:        { row: 21, col: 44 },
  facilityEpaId:      { row: 21, col: 52 },
  // Box 9a-9d - Waste lines (4 lines)
  waste1hm:           { row: 25, col: 3 },
  waste1desc:         { row: 25, col: 7 },
  waste1code:         { row: 25, col: 42 },
  waste1container:    { row: 25, col: 47 },
  waste1qty:          { row: 25, col: 51 },
  waste1uom:          { row: 25, col: 57 },
  waste1wCodes:       { row: 25, col: 62 },
  waste2hm:           { row: 27, col: 3 },
  waste2desc:         { row: 27, col: 7 },
  waste2code:         { row: 27, col: 42 },
  waste2container:    { row: 27, col: 47 },
  waste2qty:          { row: 27, col: 51 },
  waste2uom:          { row: 27, col: 57 },
  waste2wCodes:       { row: 27, col: 62 },
  waste3hm:           { row: 29, col: 3 },
  waste3desc:         { row: 29, col: 7 },
  waste3code:         { row: 29, col: 42 },
  waste3container:    { row: 29, col: 47 },
  waste3qty:          { row: 29, col: 51 },
  waste3uom:          { row: 29, col: 57 },
  waste3wCodes:       { row: 29, col: 62 },
  waste4hm:           { row: 31, col: 3 },
  waste4desc:         { row: 31, col: 7 },
  waste4code:         { row: 31, col: 42 },
  waste4container:    { row: 31, col: 47 },
  waste4qty:          { row: 31, col: 51 },
  waste4uom:          { row: 31, col: 57 },
  waste4wCodes:       { row: 31, col: 62 },
  // Box 14 - Special Handling (4 lines)
  specialHandling:    { row: 34, col: 7 },
  specialHandling2:   { row: 35, col: 7 },
  specialHandling3:   { row: 36, col: 7 },
  specialHandling4:   { row: 37, col: 7 },
  // Box 15 - Generator Certification
  generatorCertName:  { row: 39, col: 7 },
  certDate:           { row: 39, col: 52 }
};

// Print manifest - plain text for dot matrix
// Uses manifest fields directly (as saved by the frontend)
app.get('/api/print/manifest/:id', function(req, res) {
  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).send('Manifest not found');

  var lines = [];
  for (var l = 0; l < 66; l++) {
    var row = '';
    for (var c = 0; c < 80; c++) { row += ' '; }
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
  placeText(FORM_8700_MAP.generatorEpaId.row, FORM_8700_MAP.generatorEpaId.col, manifest.generatorEpaId);
  // Box 2 - Page
  placeText(FORM_8700_MAP.page.row, FORM_8700_MAP.page.col, manifest.pageNum || '1');
  placeText(FORM_8700_MAP.totalPages.row, FORM_8700_MAP.totalPages.col, manifest.pageTotal || '1');
  // Box 3 - Emergency Response Phone
  placeText(FORM_8700_MAP.emergencyPhone.row, FORM_8700_MAP.emergencyPhone.col, manifest.emergencyPhone);

  // Box 5 - Generator
  placeText(FORM_8700_MAP.generatorName.row, FORM_8700_MAP.generatorName.col, manifest.generatorName);
  placeText(FORM_8700_MAP.generatorPhone.row, FORM_8700_MAP.generatorPhone.col, manifest.generatorPhone);
  // Mailing address (LEFT side of Box 5)
  placeText(FORM_8700_MAP.generatorMailAddr.row, FORM_8700_MAP.generatorMailAddr.col, manifest.generatorMailAddress);
  placeText(FORM_8700_MAP.generatorMailCity.row, FORM_8700_MAP.generatorMailCity.col, manifest.generatorMailCityStZip);
  // Site address (RIGHT side of Box 5 - same rows, higher column)
  placeText(FORM_8700_MAP.generatorSiteAddr.row, FORM_8700_MAP.generatorSiteAddr.col, manifest.genSiteAddress);
  placeText(FORM_8700_MAP.generatorSiteCity.row, FORM_8700_MAP.generatorSiteCity.col, manifest.genSiteCityStZip);

  // Box 6 - Transporter 1
  placeText(FORM_8700_MAP.transporter1Name.row, FORM_8700_MAP.transporter1Name.col, manifest.transporter1Name);
  placeText(FORM_8700_MAP.transporter1EpaId.row, FORM_8700_MAP.transporter1EpaId.col, manifest.transporter1EpaId);
  // Box 7 - Transporter 2
  placeText(FORM_8700_MAP.transporter2Name.row, FORM_8700_MAP.transporter2Name.col, manifest.transporter2Name);
  placeText(FORM_8700_MAP.transporter2EpaId.row, FORM_8700_MAP.transporter2EpaId.col, manifest.transporter2EpaId);

  // Box 8 - Facility
  placeText(FORM_8700_MAP.facilityName.row, FORM_8700_MAP.facilityName.col, manifest.facilityName);
  placeText(FORM_8700_MAP.facilityPhone.row, FORM_8700_MAP.facilityPhone.col, manifest.facilityPhone);
  placeText(FORM_8700_MAP.facilityAddress.row, FORM_8700_MAP.facilityAddress.col, manifest.facilityAddress);
  placeText(FORM_8700_MAP.facilityCity.row, FORM_8700_MAP.facilityCity.col, manifest.facilityCityStZip);
  placeText(FORM_8700_MAP.facilityEpaId.row, FORM_8700_MAP.facilityEpaId.col, manifest.facilityEpaId);

  // Box 9 - Waste lines (uses flattened fields: waste1Description, waste1ContainerType, etc.)
  // Row 1 description: col 7 to col 41 (35 chars) - other fields start at col 42
  // Rows 2-3 continuation: col 7 to col 73 (67 chars) - full width, no other fields
  var descRow1Width = FORM_8700_MAP.waste1code.col - FORM_8700_MAP.waste1desc.col - 1;
  var descContWidth = 67;
  function wrapDescLines(text, firstMax, contMax) {
    if (!text) return [];
    var result = [];
    var remaining = String(text);
    // First line - limited width
    if (remaining.length <= firstMax) { return [remaining]; }
    var cut = remaining.lastIndexOf(' ', firstMax);
    if (cut <= 0) cut = firstMax;
    result.push(remaining.substring(0, cut));
    remaining = remaining.substring(cut).replace(/^\s+/, '');
    // Continuation lines - full width
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
    var baseRow = FORM_8700_MAP['waste' + w + 'desc'].row;
    for (var dl = 0; dl < descLines.length && dl < 3; dl++) {
      placeText(baseRow + dl, FORM_8700_MAP['waste' + w + 'desc'].col, descLines[dl]);
    }
    placeText(FORM_8700_MAP['waste' + w + 'hm'].row, FORM_8700_MAP['waste' + w + 'hm'].col, manifest['waste' + w + 'HM']);
    placeText(FORM_8700_MAP['waste' + w + 'container'].row, FORM_8700_MAP['waste' + w + 'container'].col, manifest['waste' + w + 'ContainerType']);
    placeText(FORM_8700_MAP['waste' + w + 'qty'].row, FORM_8700_MAP['waste' + w + 'qty'].col, manifest['waste' + w + 'Qty']);
    placeText(FORM_8700_MAP['waste' + w + 'uom'].row, FORM_8700_MAP['waste' + w + 'uom'].col, manifest['waste' + w + 'Unit']);
    placeText(FORM_8700_MAP['waste' + w + 'wCodes'].row, FORM_8700_MAP['waste' + w + 'wCodes'].col, manifest['waste' + w + 'WasteCodes']);
    placeText(FORM_8700_MAP['waste' + w + 'code'].row, FORM_8700_MAP['waste' + w + 'code'].col, manifest['waste' + w + 'ContainerNum']);
  }

  // Box 14 - Special Handling (4 lines)
  placeText(FORM_8700_MAP.specialHandling.row, FORM_8700_MAP.specialHandling.col, manifest.specialHandling);
  placeText(FORM_8700_MAP.specialHandling2.row, FORM_8700_MAP.specialHandling2.col, manifest.specialHandling2);
  placeText(FORM_8700_MAP.specialHandling3.row, FORM_8700_MAP.specialHandling3.col, manifest.specialHandling3);
  placeText(FORM_8700_MAP.specialHandling4.row, FORM_8700_MAP.specialHandling4.col, manifest.specialHandling4);

  // Box 15 - Generator Certification
  placeText(FORM_8700_MAP.generatorCertName.row, FORM_8700_MAP.generatorCertName.col, manifest.generatorPrintName);
  placeText(FORM_8700_MAP.certDate.row, FORM_8700_MAP.certDate.col, manifest.generatorDate);

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

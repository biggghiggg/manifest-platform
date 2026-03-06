var express = require('express');
var multer = require('multer');
var XLSX = require('xlsx');
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
      // Try to match: street, city, state zip
      var parts = str.split(',');
      if (parts.length >= 2) {
        var street = parts[0].trim();
        var rest = parts.slice(1).join(',').trim();
        // Try state + zip at end
        var stateZipMatch = rest.match(/\s*([A-Za-z]{2})\s+(\d{5}(-\d{4})?)\s*$/);
        if (stateZipMatch) {
          var city = rest.substring(0, rest.length - stateZipMatch[0].length).trim().replace(/,\s*$/, '');
          return { street: street, city: city, state: stateZipMatch[1].toUpperCase(), zip: stateZipMatch[2] };
        }
        // fallback: last part might be "City ST ZIP"
        var fallback = rest.match(/^(.+?)\s+([A-Za-z]{2})\s+(\d{5}(-\d{4})?)/);
        if (fallback) {
          return { street: street, city: fallback[1].trim(), state: fallback[2].toUpperCase(), zip: fallback[3] };
        }
        return { street: street, city: rest, state: '', zip: '' };
      }
      // No commas - try "Street City ST 00000"
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

      // Check for duplicates by name
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

    // Clean up uploaded file
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
  waste1desc:         { row: 25, col: 7 },
  waste1code:         { row: 25, col: 42 },
  waste1container:    { row: 25, col: 47 },
  waste1qty:          { row: 25, col: 51 },
  waste1uom:          { row: 25, col: 57 },
  waste1wCodes:       { row: 25, col: 62 },
  waste2desc:         { row: 27, col: 7 },
  waste2code:         { row: 27, col: 42 },
  waste2container:    { row: 27, col: 47 },
  waste2qty:          { row: 27, col: 51 },
  waste2uom:          { row: 27, col: 57 },
  waste2wCodes:       { row: 27, col: 62 },
  waste3desc:         { row: 29, col: 7 },
  waste3code:         { row: 29, col: 42 },
  waste3container:    { row: 29, col: 47 },
  waste3qty:          { row: 29, col: 51 },
  waste3uom:          { row: 29, col: 57 },
  waste3wCodes:       { row: 29, col: 62 },
  waste4desc:         { row: 31, col: 7 },
  waste4code:         { row: 31, col: 42 },
  waste4container:    { row: 31, col: 47 },
  waste4qty:          { row: 31, col: 51 },
  waste4uom:          { row: 31, col: 57 },
  waste4wCodes:       { row: 31, col: 62 },
  // Box 14 - Special Handling
  specialHandling:    { row: 34, col: 7 },
  // Box 15 - Generator Certification
  generatorCertName:  { row: 39, col: 7 },
  certDate:           { row: 39, col: 52 }
};

// Print manifest - plain text for dot matrix
app.get('/api/print/manifest/:id', function(req, res) {
  var manifest = null;
  for (var i = 0; i < data.manifests.length; i++) {
    if (data.manifests[i].id === req.params.id) { manifest = data.manifests[i]; break; }
  }
  if (!manifest) return res.status(404).send('Manifest not found');

  // Build 66-line page (80 cols each)
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

  // Look up generator
  var gen = null;
  if (manifest.generatorId) {
    for (var g = 0; g < data.generators.length; g++) {
      if (data.generators[g].id === manifest.generatorId) { gen = data.generators[g]; break; }
    }
  }

  // Look up transporters
  var trans1 = null, trans2 = null;
  if (manifest.transporter1Id) {
    for (var t = 0; t < data.transporters.length; t++) {
      if (data.transporters[t].id === manifest.transporter1Id) { trans1 = data.transporters[t]; break; }
    }
  }
  if (manifest.transporter2Id) {
    for (var t2 = 0; t2 < data.transporters.length; t2++) {
      if (data.transporters[t2].id === manifest.transporter2Id) { trans2 = data.transporters[t2]; break; }
    }
  }

  // Look up facility
  var fac = null;
  if (manifest.facilityId) {
    for (var f = 0; f < data.facilities.length; f++) {
      if (data.facilities[f].id === manifest.facilityId) { fac = data.facilities[f]; break; }
    }
  }

  // Box 1 - Generator EPA ID
  if (gen) placeText(FORM_8700_MAP.generatorEpaId.row, FORM_8700_MAP.generatorEpaId.col, gen.epaId);

  // Box 2 - Page
  placeText(FORM_8700_MAP.page.row, FORM_8700_MAP.page.col, manifest.page || '1');
  placeText(FORM_8700_MAP.totalPages.row, FORM_8700_MAP.totalPages.col, manifest.totalPages || '1');

  // Box 3 - Emergency Response Phone
  if (gen) placeText(FORM_8700_MAP.emergencyPhone.row, FORM_8700_MAP.emergencyPhone.col, gen.emergencyPhone || gen.phone);

  // Box 5 - Generator info
  if (gen) {
    placeText(FORM_8700_MAP.generatorName.row, FORM_8700_MAP.generatorName.col, gen.name);
    placeText(FORM_8700_MAP.generatorPhone.row, FORM_8700_MAP.generatorPhone.col, gen.phone);
    // Mailing address (LEFT)
    placeText(FORM_8700_MAP.generatorMailAddr.row, FORM_8700_MAP.generatorMailAddr.col, gen.mailAddress);
    var mailCityLine = (gen.mailCity || '') + ', ' + (gen.mailState || '') + ', ' + (gen.mailZip || '');
    placeText(FORM_8700_MAP.generatorMailCity.row, FORM_8700_MAP.generatorMailCity.col, mailCityLine.replace(/^, |, $/g, ''));
    // Site address (RIGHT - same rows, higher column)
    placeText(FORM_8700_MAP.generatorSiteAddr.row, FORM_8700_MAP.generatorSiteAddr.col, gen.siteAddress);
    var siteCityLine = (gen.city || '') + ', ' + (gen.state || '') + ', ' + (gen.zip || '');
    placeText(FORM_8700_MAP.generatorSiteCity.row, FORM_8700_MAP.generatorSiteCity.col, siteCityLine.replace(/^, |, $/g, ''));
  }

  // Box 6 - Transporter 1
  if (trans1) {
    placeText(FORM_8700_MAP.transporter1Name.row, FORM_8700_MAP.transporter1Name.col, trans1.name);
    placeText(FORM_8700_MAP.transporter1EpaId.row, FORM_8700_MAP.transporter1EpaId.col, trans1.epaId);
  }

  // Box 7 - Transporter 2
  if (trans2) {
    placeText(FORM_8700_MAP.transporter2Name.row, FORM_8700_MAP.transporter2Name.col, trans2.name);
    placeText(FORM_8700_MAP.transporter2EpaId.row, FORM_8700_MAP.transporter2EpaId.col, trans2.epaId);
  }

  // Box 8 - Facility
  if (fac) {
    placeText(FORM_8700_MAP.facilityName.row, FORM_8700_MAP.facilityName.col, fac.name);
    placeText(FORM_8700_MAP.facilityPhone.row, FORM_8700_MAP.facilityPhone.col, fac.phone);
    placeText(FORM_8700_MAP.facilityAddress.row, FORM_8700_MAP.facilityAddress.col, fac.siteAddress);
    var facCityLine = (fac.city || '') + ', ' + (fac.state || '') + ', ' + (fac.zip || '');
    placeText(FORM_8700_MAP.facilityCity.row, FORM_8700_MAP.facilityCity.col, facCityLine.replace(/^, |, $/g, ''));
    placeText(FORM_8700_MAP.facilityEpaId.row, FORM_8700_MAP.facilityEpaId.col, fac.epaId);
  }

  // Box 9 - Waste lines
  var wasteLines = manifest.wasteLines || [];
  for (var w = 0; w < Math.min(wasteLines.length, 4); w++) {
    var wl = wasteLines[w];
    var n = w + 1;
    if (wl.description) placeText(FORM_8700_MAP['waste' + n + 'desc'].row, FORM_8700_MAP['waste' + n + 'desc'].col, wl.description);
    if (wl.dotCode) placeText(FORM_8700_MAP['waste' + n + 'code'].row, FORM_8700_MAP['waste' + n + 'code'].col, wl.dotCode);
    if (wl.containerType) placeText(FORM_8700_MAP['waste' + n + 'container'].row, FORM_8700_MAP['waste' + n + 'container'].col, wl.containerType);
    if (wl.quantity) placeText(FORM_8700_MAP['waste' + n + 'qty'].row, FORM_8700_MAP['waste' + n + 'qty'].col, String(wl.quantity));
    if (wl.unitOfMeasure) placeText(FORM_8700_MAP['waste' + n + 'uom'].row, FORM_8700_MAP['waste' + n + 'uom'].col, wl.unitOfMeasure);
    if (wl.wasteCodes) placeText(FORM_8700_MAP['waste' + n + 'wCodes'].row, FORM_8700_MAP['waste' + n + 'wCodes'].col, wl.wasteCodes);
  }

  // Box 14 - Special Handling
  if (manifest.specialHandling) {
    placeText(FORM_8700_MAP.specialHandling.row, FORM_8700_MAP.specialHandling.col, manifest.specialHandling);
  }

  // Box 15 - Certification
  if (manifest.certName) placeText(FORM_8700_MAP.generatorCertName.row, FORM_8700_MAP.generatorCertName.col, manifest.certName);
  var today = new Date().toLocaleDateString('en-US');
  placeText(FORM_8700_MAP.certDate.row, FORM_8700_MAP.certDate.col, manifest.certDate || today);

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
  // Redirect to text version for now
  res.redirect('/api/print/manifest/' + req.params.id);
});

// Serve static files
app.use(express.static(path.join(__dirname)));

// Start server
app.listen(PORT, function() {
  console.log('Manifest Platform running on port ' + PORT);
  console.log('Data directory: ' + DATA_DIR);
});

const express = require('express');
const fs = require('fs');
const path = require('path');
const multer = require('multer');
const XLSX = require('xlsx');

const app = express();
const PORT = process.env.PORT || 3000;

const DATA_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH || path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const DATA_FILE = path.join(DATA_DIR, 'manifest-data.json');
const upload = multer({ dest: path.join(DATA_DIR, 'uploads') });

let sseClients = [];

function getDefaultData() {
  return { generators: [], transporters: [], facilities: [], wasteStreams: [], manifests: [], nextManifestNum: 1 };
}

function loadData() {
  try {
    if (fs.existsSync(DATA_FILE)) return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  } catch (e) { console.error('Error loading data:', e); }
  return getDefaultData();
}

function saveData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  broadcastSSE({ type: 'data-updated' });
}

function broadcastSSE(event) {
  var msg = 'data: ' + JSON.stringify(event) + '\n\n';
  sseClients.forEach(function(res) { res.write(msg); });
}

app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname)));

app.get('/api/events', function(req, res) {
  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });
  res.write('data: {"type":"connected"}\n\n');
  sseClients.push(res);
  req.on('close', function() { sseClients = sseClients.filter(function(c) { return c !== res; }); });
});

app.get('/api/data', function(req, res) { res.json(loadData()); });

// --- Generators ---
app.get('/api/generators', function(req, res) { res.json(loadData().generators); });
app.post('/api/generators', function(req, res) {
  var data = loadData();
  var gen = Object.assign({ id: Date.now().toString(), createdAt: new Date().toISOString() }, req.body);
  data.generators.push(gen);
  saveData(data);
  res.json(gen);
});
app.put('/api/generators/:id', function(req, res) {
  var data = loadData();
  var idx = data.generators.findIndex(function(g) { return g.id === req.params.id; });
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  data.generators[idx] = Object.assign({}, data.generators[idx], req.body);
  saveData(data);
  res.json(data.generators[idx]);
});
app.delete('/api/generators/:id', function(req, res) {
  var data = loadData();
  data.generators = data.generators.filter(function(g) { return g.id !== req.params.id; });
  saveData(data);
  res.json({ ok: true });
});
// Clear ALL generators
app.delete('/api/generators', function(req, res) {
  var data = loadData();
  data.generators = [];
  saveData(data);
  res.json({ ok: true, cleared: true });
});

// --- Transporters ---
app.get('/api/transporters', function(req, res) { res.json(loadData().transporters); });
app.post('/api/transporters', function(req, res) {
  var data = loadData();
  var t = Object.assign({ id: Date.now().toString(), createdAt: new Date().toISOString() }, req.body);
  data.transporters.push(t);
  saveData(data);
  res.json(t);
});
app.put('/api/transporters/:id', function(req, res) {
  var data = loadData();
  var idx = data.transporters.findIndex(function(t) { return t.id === req.params.id; });
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  data.transporters[idx] = Object.assign({}, data.transporters[idx], req.body);
  saveData(data);
  res.json(data.transporters[idx]);
});
app.delete('/api/transporters/:id', function(req, res) {
  var data = loadData();
  data.transporters = data.transporters.filter(function(t) { return t.id !== req.params.id; });
  saveData(data);
  res.json({ ok: true });
});

// --- Facilities ---
app.get('/api/facilities', function(req, res) { res.json(loadData().facilities); });
app.post('/api/facilities', function(req, res) {
  var data = loadData();
  var f = Object.assign({ id: Date.now().toString(), createdAt: new Date().toISOString() }, req.body);
  data.facilities.push(f);
  saveData(data);
  res.json(f);
});
app.put('/api/facilities/:id', function(req, res) {
  var data = loadData();
  var idx = data.facilities.findIndex(function(f) { return f.id === req.params.id; });
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  data.facilities[idx] = Object.assign({}, data.facilities[idx], req.body);
  saveData(data);
  res.json(data.facilities[idx]);
});
app.delete('/api/facilities/:id', function(req, res) {
  var data = loadData();
  data.facilities = data.facilities.filter(function(f) { return f.id !== req.params.id; });
  saveData(data);
  res.json({ ok: true });
});

// --- Waste Streams ---
app.get('/api/waste-streams', function(req, res) { res.json(loadData().wasteStreams); });
app.post('/api/waste-streams', function(req, res) {
  var data = loadData();
  var w = Object.assign({ id: Date.now().toString(), createdAt: new Date().toISOString() }, req.body);
  data.wasteStreams.push(w);
  saveData(data);
  res.json(w);
});
app.put('/api/waste-streams/:id', function(req, res) {
  var data = loadData();
  var idx = data.wasteStreams.findIndex(function(w) { return w.id === req.params.id; });
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  data.wasteStreams[idx] = Object.assign({}, data.wasteStreams[idx], req.body);
  saveData(data);
  res.json(data.wasteStreams[idx]);
});
app.delete('/api/waste-streams/:id', function(req, res) {
  var data = loadData();
  data.wasteStreams = data.wasteStreams.filter(function(w) { return w.id !== req.params.id; });
  saveData(data);
  res.json({ ok: true });
});

// --- Manifests ---
app.get('/api/manifests', function(req, res) { res.json(loadData().manifests); });
app.post('/api/manifests', function(req, res) {
  var data = loadData();
  var m = Object.assign({ id: Date.now().toString(), manifestNum: data.nextManifestNum++, createdAt: new Date().toISOString(), status: 'draft' }, req.body);
  data.manifests.push(m);
  saveData(data);
  res.json(m);
});
app.put('/api/manifests/:id', function(req, res) {
  var data = loadData();
  var idx = data.manifests.findIndex(function(m) { return m.id === req.params.id; });
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  data.manifests[idx] = Object.assign({}, data.manifests[idx], req.body);
  saveData(data);
  res.json(data.manifests[idx]);
});
app.delete('/api/manifests/:id', function(req, res) {
  var data = loadData();
  data.manifests = data.manifests.filter(function(m) { return m.id !== req.params.id; });
  saveData(data);
  res.json({ ok: true });
});

// ============ QUICKBOOKS IMPORT ============
function parseAddress(addr) {
  if (!addr || typeof addr !== 'string') return { street: '', city: '', state: '', zip: '' };
  addr = addr.trim();
  var parts = addr.split(/\s+/);
  if (parts.length < 3) return { street: addr, city: '', state: '', zip: '' };
  var zip = parts[parts.length - 1];
  var state = parts[parts.length - 2];
  if (/^\d{5}(-\d{4})?$/.test(zip) && /^[A-Za-z]{2}$/.test(state)) {
    var cityEnd = parts.length - 2;
    var cityStart = cityEnd - 1;
    if (cityStart > 0 && /^[A-Z][a-z]/.test(parts[cityStart - 1]) && !/^\d/.test(parts[cityStart - 1])) {
      var streetSuffixes = ['St','St.','Ave','Ave.','Blvd','Blvd.','Dr','Dr.','Rd','Rd.','Ln','Ln.','Way','Ct','Ct.','Pl','Pl.','Circle','Pkwy'];
      if (streetSuffixes.indexOf(parts[cityStart - 1]) === -1) {
        cityStart = cityStart - 1;
      }
    }
    return { street: parts.slice(0, cityStart).join(' '), city: parts.slice(cityStart, cityEnd).join(' '), state: state, zip: zip };
  }
  return { street: addr, city: '', state: '', zip: '' };
}

function parsePhone(phoneStr) {
  if (!phoneStr || typeof phoneStr !== 'string') return '';
  var match = phoneStr.match(/\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}/);
  return match ? match[0] : phoneStr.trim();
}

app.post('/api/import/quickbooks', upload.single('file'), function(req, res) {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
    var workbook = XLSX.readFile(req.file.path);
    var sheet = workbook.Sheets[workbook.SheetNames[0]];
    var allRows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });

    var headerIdx = -1;
    var headers = [];
    for (var i = 0; i < Math.min(allRows.length, 20); i++) {
      var row = allRows[i];
      if (row && row.some(function(cell) {
        var s = String(cell || '').toLowerCase();
        return s.indexOf('customer') >= 0 && (s.indexOf('name') >= 0 || s.indexOf('full') >= 0);
      })) {
        headerIdx = i;
        headers = row.map(function(h) { return String(h || '').trim(); });
        break;
      }
    }
    if (headerIdx === -1) { headerIdx = 0; headers = allRows[0].map(function(h) { return String(h || '').trim(); }); }

    function findCol() {
      var keywords = Array.from(arguments);
      return headers.findIndex(function(h) {
        var lower = h.toLowerCase();
        return keywords.some(function(kw) { return lower.indexOf(kw) >= 0; });
      });
    }

    var nameCol = findCol('customer', 'name');
    var phoneCol = findCol('phone');
    var billAddrCol = findCol('bill');
    var shipAddrCol = findCol('ship');
    var epaCol = findCol('epa');

    var data = loadData();
    var imported = 0;
    var skipped = 0;

    for (var i = headerIdx + 1; i < allRows.length; i++) {
      var row = allRows[i];
      if (!row || row.every(function(cell) { return !cell && cell !== 0; })) continue;
      var name = nameCol >= 0 ? String(row[nameCol] || '').trim() : '';
      if (!name) continue;
      if (data.generators.find(function(g) { return g.name && g.name.toLowerCase() === name.toLowerCase(); })) { skipped++; continue; }

      var shipAddrRaw = shipAddrCol >= 0 ? String(row[shipAddrCol] || '') : '';
      var billAddrRaw = billAddrCol >= 0 ? String(row[billAddrCol] || '') : '';
      var siteAddr = parseAddress(shipAddrRaw || billAddrRaw);
      var mailAddr = parseAddress(billAddrRaw);
      var phone = phoneCol >= 0 ? parsePhone(String(row[phoneCol] || '')) : '';
      var epaId = epaCol >= 0 ? String(row[epaCol] || '').trim() : '';

      data.generators.push({
        id: Date.now().toString() + '_' + imported,
        name: name, epaId: epaId,
        siteAddress: siteAddr.street, city: siteAddr.city, state: siteAddr.state, zip: siteAddr.zip,
        mailAddress: mailAddr.street, mailCity: mailAddr.city, mailState: mailAddr.state, mailZip: mailAddr.zip,
        phone: phone, contactName: '', emergencyPhone: '',
        fullShipAddress: shipAddrRaw.trim(), fullBillAddress: billAddrRaw.trim(),
        createdAt: new Date().toISOString(), source: 'quickbooks'
      });
      imported++;
    }

    saveData(data);
    try { fs.unlinkSync(req.file.path); } catch(e) {}
    res.json({ imported: imported, skipped: skipped, total: allRows.length - headerIdx - 1 });
  } catch (e) {
    console.error('Import error:', e);
    res.status(500).json({ error: 'Failed to import file: ' + e.message });
  }
});

// ============ DOT MATRIX PRINT ============
var FORM_8700_MAP = {
  generatorEpaId:         { row: 5,  col: 28, maxLen: 12 },
  pageNum:                { row: 5,  col: 68, maxLen: 1 },
  pageTotal:              { row: 5,  col: 73, maxLen: 1 },
  emergencyPhone:         { row: 7,  col: 28, maxLen: 20 },
  manifestTrackingNum:    { row: 5,  col: 50, maxLen: 12 },
  generatorName:          { row: 9,  col: 8,  maxLen: 35 },
  generatorMailAddress:   { row: 10, col: 8,  maxLen: 35 },
  generatorMailCityStZip: { row: 11, col: 8,  maxLen: 35 },
  generatorPhone:         { row: 9,  col: 55, maxLen: 20 },
  genSiteAddress:         { row: 12, col: 8,  maxLen: 35 },
  genSiteCityStZip:       { row: 13, col: 8,  maxLen: 35 },
  transporter1Name:       { row: 15, col: 8,  maxLen: 35 },
  transporter1EpaId:      { row: 15, col: 55, maxLen: 12 },
  transporter2Name:       { row: 17, col: 8,  maxLen: 35 },
  transporter2EpaId:      { row: 17, col: 55, maxLen: 12 },
  facilityName:           { row: 19, col: 8,  maxLen: 35 },
  facilityAddress:        { row: 20, col: 8,  maxLen: 35 },
  facilityCityStZip:      { row: 21, col: 8,  maxLen: 35 },
  facilityPhone:          { row: 19, col: 55, maxLen: 20 },
  facilityEpaId:          { row: 21, col: 55, maxLen: 12 },
  waste1HM: { row: 25, col: 3, maxLen: 1 }, waste1Description: { row: 25, col: 8, maxLen: 32 },
  waste1ContainerNum: { row: 25, col: 42, maxLen: 4 }, waste1ContainerType: { row: 25, col: 47, maxLen: 2 },
  waste1Qty: { row: 25, col: 51, maxLen: 6 }, waste1Unit: { row: 25, col: 58, maxLen: 1 }, waste1WasteCodes: { row: 25, col: 62, maxLen: 16 },
  waste2HM: { row: 27, col: 3, maxLen: 1 }, waste2Description: { row: 27, col: 8, maxLen: 32 },
  waste2ContainerNum: { row: 27, col: 42, maxLen: 4 }, waste2ContainerType: { row: 27, col: 47, maxLen: 2 },
  waste2Qty: { row: 27, col: 51, maxLen: 6 }, waste2Unit: { row: 27, col: 58, maxLen: 1 }, waste2WasteCodes: { row: 27, col: 62, maxLen: 16 },
  waste3HM: { row: 29, col: 3, maxLen: 1 }, waste3Description: { row: 29, col: 8, maxLen: 32 },
  waste3ContainerNum: { row: 29, col: 42, maxLen: 4 }, waste3ContainerType: { row: 29, col: 47, maxLen: 2 },
  waste3Qty: { row: 29, col: 51, maxLen: 6 }, waste3Unit: { row: 29, col: 58, maxLen: 1 }, waste3WasteCodes: { row: 29, col: 62, maxLen: 16 },
  waste4HM: { row: 31, col: 3, maxLen: 1 }, waste4Description: { row: 31, col: 8, maxLen: 32 },
  waste4ContainerNum: { row: 31, col: 42, maxLen: 4 }, waste4ContainerType: { row: 31, col: 47, maxLen: 2 },
  waste4Qty: { row: 31, col: 51, maxLen: 6 }, waste4Unit: { row: 31, col: 58, maxLen: 1 }, waste4WasteCodes: { row: 31, col: 62, maxLen: 16 },
  specialHandling:  { row: 34, col: 8, maxLen: 65 },
  specialHandling2: { row: 35, col: 8, maxLen: 65 },
  generatorPrintName: { row: 39, col: 8, maxLen: 30 },
  generatorDate:      { row: 39, col: 55, maxLen: 10 }
};

function buildPrintData(manifest) {
  var lines = [];
  for (var i = 0; i < 66; i++) lines.push('                                                                                ');
  for (var field in FORM_8700_MAP) {
    var pos = FORM_8700_MAP[field];
    var value = manifest[field] || '';
    if (typeof value !== 'string') value = String(value);
    value = value.substring(0, pos.maxLen);
    if (value) {
      var row = pos.row - 1;
      var col = pos.col - 1;
      var line = lines[row];
      lines[row] = line.substring(0, col) + value + line.substring(col + value.length);
    }
  }
  return lines.join('\r\n');
}

app.post('/api/print/manifest/:id', function(req, res) {
  var data = loadData();
  var manifest = data.manifests.find(function(m) { return m.id === req.params.id; });
  if (!manifest) return res.status(404).json({ error: 'Manifest not found' });
  var printData = buildPrintData(manifest);
  res.setHeader('Content-Type', 'text/plain');
  res.setHeader('Content-Disposition', 'inline; filename="manifest-' + manifest.manifestNum + '.txt"');
  res.send(printData);
});

app.get('/api/print/raw/:id', function(req, res) {
  var data = loadData();
  var manifest = data.manifests.find(function(m) { return m.id === req.params.id; });
  if (!manifest) return res.status(404).json({ error: 'Manifest not found' });
  var printData = buildPrintData(manifest);
  res.setHeader('Content-Type', 'application/octet-stream');
  res.setHeader('Content-Disposition', 'attachment; filename="manifest-' + manifest.manifestNum + '.prn"');
  res.send(printData);
});

app.listen(PORT, function() {
  console.log('Manifest Platform running on port ' + PORT);
});

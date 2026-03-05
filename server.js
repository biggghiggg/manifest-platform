const express = require('express');
const fs = require('fs');
const path = require('path');
const multer = require('multer');
const XLSX = require('xlsx');

const app = express();
const PORT = process.env.PORT || 3000;

// Data directory - use Railway volume if available
const DATA_DIR = process.env.RAILWAY_VOLUME_MOUNT_PATH || path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const DATA_FILE = path.join(DATA_DIR, 'manifest-data.json');
const upload = multer({ dest: path.join(DATA_DIR, 'uploads') });

// SSE clients
let sseClients = [];

// Default data structure
function getDefaultData() {
  return {
    generators: [],
    transporters: [],
    facilities: [],
    wasteStreams: [],
    manifests: [],
    nextManifestNum: 1
  };
}

// Load / save data
function loadData() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    }
  } catch (e) {
    console.error('Error loading data:', e);
  }
  return getDefaultData();
}

function saveData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  broadcastSSE({ type: 'data-updated' });
}

// SSE broadcast
function broadcastSSE(event) {
  const msg = `data: ${JSON.stringify(event)}\n\n`;
  sseClients.forEach(res => res.write(msg));
}

// Middleware
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname)));

// SSE endpoint
app.get('/api/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive'
  });
  res.write('data: {"type":"connected"}\n\n');
  sseClients.push(res);
  req.on('close', () => {
    sseClients = sseClients.filter(c => c !== res);
  });
});

// ============ API ROUTES ============

// Get all data
app.get('/api/data', (req, res) => {
  res.json(loadData());
});

// --- Generators ---
app.get('/api/generators', (req, res) => {
  res.json(loadData().generators);
});

app.post('/api/generators', (req, res) => {
  const data = loadData();
  const gen = { id: Date.now().toString(), ...req.body, createdAt: new Date().toISOString() };
  data.generators.push(gen);
  saveData(data);
  res.json(gen);
});

app.put('/api/generators/:id', (req, res) => {
  const data = loadData();
  const idx = data.generators.findIndex(g => g.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  data.generators[idx] = { ...data.generators[idx], ...req.body };
  saveData(data);
  res.json(data.generators[idx]);
});

app.delete('/api/generators/:id', (req, res) => {
  const data = loadData();
  data.generators = data.generators.filter(g => g.id !== req.params.id);
  saveData(data);
  res.json({ ok: true });
});

// --- Transporters ---
app.get('/api/transporters', (req, res) => {
  res.json(loadData().transporters);
});

app.post('/api/transporters', (req, res) => {
  const data = loadData();
  const t = { id: Date.now().toString(), ...req.body, createdAt: new Date().toISOString() };
  data.transporters.push(t);
  saveData(data);
  res.json(t);
});

app.put('/api/transporters/:id', (req, res) => {
  const data = loadData();
  const idx = data.transporters.findIndex(t => t.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  data.transporters[idx] = { ...data.transporters[idx], ...req.body };
  saveData(data);
  res.json(data.transporters[idx]);
});

app.delete('/api/transporters/:id', (req, res) => {
  const data = loadData();
  data.transporters = data.transporters.filter(t => t.id !== req.params.id);
  saveData(data);
  res.json({ ok: true });
});

// --- Facilities (TSDF) ---
app.get('/api/facilities', (req, res) => {
  res.json(loadData().facilities);
});

app.post('/api/facilities', (req, res) => {
  const data = loadData();
  const f = { id: Date.now().toString(), ...req.body, createdAt: new Date().toISOString() };
  data.facilities.push(f);
  saveData(data);
  res.json(f);
});

app.put('/api/facilities/:id', (req, res) => {
  const data = loadData();
  const idx = data.facilities.findIndex(f => f.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  data.facilities[idx] = { ...data.facilities[idx], ...req.body };
  saveData(data);
  res.json(data.facilities[idx]);
});

app.delete('/api/facilities/:id', (req, res) => {
  const data = loadData();
  data.facilities = data.facilities.filter(f => f.id !== req.params.id);
  saveData(data);
  res.json({ ok: true });
});

// --- Waste Streams ---
app.get('/api/waste-streams', (req, res) => {
  res.json(loadData().wasteStreams);
});

app.post('/api/waste-streams', (req, res) => {
  const data = loadData();
  const w = { id: Date.now().toString(), ...req.body, createdAt: new Date().toISOString() };
  data.wasteStreams.push(w);
  saveData(data);
  res.json(w);
});

app.put('/api/waste-streams/:id', (req, res) => {
  const data = loadData();
  const idx = data.wasteStreams.findIndex(w => w.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  data.wasteStreams[idx] = { ...data.wasteStreams[idx], ...req.body };
  saveData(data);
  res.json(data.wasteStreams[idx]);
});

app.delete('/api/waste-streams/:id', (req, res) => {
  const data = loadData();
  data.wasteStreams = data.wasteStreams.filter(w => w.id !== req.params.id);
  saveData(data);
  res.json({ ok: true });
});

// --- Manifests ---
app.get('/api/manifests', (req, res) => {
  res.json(loadData().manifests);
});

app.post('/api/manifests', (req, res) => {
  const data = loadData();
  const m = {
    id: Date.now().toString(),
    manifestNum: data.nextManifestNum++,
    ...req.body,
    createdAt: new Date().toISOString(),
    status: req.body.status || 'draft'
  };
  data.manifests.push(m);
  saveData(data);
  res.json(m);
});

app.put('/api/manifests/:id', (req, res) => {
  const data = loadData();
  const idx = data.manifests.findIndex(m => m.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  data.manifests[idx] = { ...data.manifests[idx], ...req.body };
  saveData(data);
  res.json(data.manifests[idx]);
});

app.delete('/api/manifests/:id', (req, res) => {
  const data = loadData();
  data.manifests = data.manifests.filter(m => m.id !== req.params.id);
  saveData(data);
  res.json({ ok: true });
});

// ============ QUICKBOOKS IMPORT ============
app.post('/api/import/quickbooks', upload.single('file'), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const workbook = XLSX.readFile(req.file.path);
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(sheet, { defval: '' });

    const data = loadData();
    let imported = 0;

    for (const row of rows) {
      // Map QuickBooks columns to generator fields
      const name = row['Customer'] || row['Company Name'] || row['Display Name'] || row['Name'] || '';
      if (!name) continue;

      // Check for duplicate
      if (data.generators.find(g => g.name.toLowerCase() === name.toLowerCase())) continue;

      const gen = {
        id: Date.now().toString() + '_' + imported,
        name: name,
        epaId: row['EPA ID'] || row['EPA Id'] || row['EPAID'] || '',
        siteAddress: row['Street'] || row['Billing Street'] || row['Ship Street'] || row['Address'] || '',
        city: row['City'] || row['Billing City'] || row['Ship City'] || '',
        state: row['State'] || row['Billing State'] || row['Ship State'] || '',
        zip: row['Zip'] || row['Billing Zip'] || row['Ship Zip'] || row['Postal Code'] || '',
        phone: row['Phone'] || row['Main Phone'] || row['Work Phone'] || '',
        contactName: row['Contact'] || row['First Name'] || row['Primary Contact'] || '',
        emergencyPhone: row['Emergency Phone'] || row['Mobile'] || row['Phone'] || '',
        createdAt: new Date().toISOString(),
        source: 'quickbooks'
      };

      data.generators.push(gen);
      imported++;
    }

    saveData(data);
    // Clean up uploaded file
    fs.unlinkSync(req.file.path);
    res.json({ imported, total: rows.length });
  } catch (e) {
    console.error('Import error:', e);
    res.status(500).json({ error: 'Failed to import file: ' + e.message });
  }
});

// ============ DOT MATRIX PRINT ENDPOINT ============
// EPA Form 8700-22 field positions for dot matrix printing
// Positions are [row, col, maxLen] based on standard 66-line continuous form
// These map to a standard 8700-22 pre-printed form at 10 CPI, 6 LPI
const FORM_8700_MAP = {
  // Box 1 - Generator's US EPA ID Number
  generatorEpaId:       { row: 5,  col: 28, maxLen: 12 },
  // Box 2 - Page __ of __
  pageNum:              { row: 5,  col: 68, maxLen: 1 },
  pageTotal:            { row: 5,  col: 73, maxLen: 1 },
  // Box 3 - Emergency Response Phone
  emergencyPhone:       { row: 7,  col: 28, maxLen: 20 },
  // Box 4 - Manifest Tracking Number (usually pre-printed, but just in case)
  manifestTrackingNum:  { row: 5,  col: 50, maxLen: 12 },
  // Box 5 - Generator Name & Mailing Address
  generatorName:        { row: 9,  col: 8,  maxLen: 35 },
  generatorAddress:     { row: 10, col: 8,  maxLen: 35 },
  generatorCityStZip:   { row: 11, col: 8,  maxLen: 35 },
  generatorPhone:       { row: 9,  col: 55, maxLen: 20 },
  // Box 5 - Generator Site Address (if different)
  genSiteAddress:       { row: 12, col: 8,  maxLen: 35 },
  genSiteCityStZip:     { row: 13, col: 8,  maxLen: 35 },
  // Box 6 - Transporter 1
  transporter1Name:     { row: 15, col: 8,  maxLen: 35 },
  transporter1EpaId:    { row: 15, col: 55, maxLen: 12 },
  // Box 7 - Transporter 2
  transporter2Name:     { row: 17, col: 8,  maxLen: 35 },
  transporter2EpaId:    { row: 17, col: 55, maxLen: 12 },
  // Box 8 - Designated Facility
  facilityName:         { row: 19, col: 8,  maxLen: 35 },
  facilityAddress:      { row: 20, col: 8,  maxLen: 35 },
  facilityCityStZip:    { row: 21, col: 8,  maxLen: 35 },
  facilityPhone:        { row: 19, col: 55, maxLen: 20 },
  facilityEpaId:        { row: 21, col: 55, maxLen: 12 },
  // Box 9 - Waste line items (up to 4)
  // Each line: DOT description, containers, qty, unit, waste codes
  // Line 1
  waste1HM:             { row: 25, col: 3,  maxLen: 1 },
  waste1Description:    { row: 25, col: 8,  maxLen: 32 },
  waste1ContainerNum:   { row: 25, col: 42, maxLen: 4 },
  waste1ContainerType:  { row: 25, col: 47, maxLen: 2 },
  waste1Qty:            { row: 25, col: 51, maxLen: 6 },
  waste1Unit:           { row: 25, col: 58, maxLen: 1 },
  waste1WasteCodes:     { row: 25, col: 62, maxLen: 16 },
  // Line 2
  waste2HM:             { row: 27, col: 3,  maxLen: 1 },
  waste2Description:    { row: 27, col: 8,  maxLen: 32 },
  waste2ContainerNum:   { row: 27, col: 42, maxLen: 4 },
  waste2ContainerType:  { row: 27, col: 47, maxLen: 2 },
  waste2Qty:            { row: 27, col: 51, maxLen: 6 },
  waste2Unit:           { row: 27, col: 58, maxLen: 1 },
  waste2WasteCodes:     { row: 27, col: 62, maxLen: 16 },
  // Line 3
  waste3HM:             { row: 29, col: 3,  maxLen: 1 },
  waste3Description:    { row: 29, col: 8,  maxLen: 32 },
  waste3ContainerNum:   { row: 29, col: 42, maxLen: 4 },
  waste3ContainerType:  { row: 29, col: 47, maxLen: 2 },
  waste3Qty:            { row: 29, col: 51, maxLen: 6 },
  waste3Unit:           { row: 29, col: 58, maxLen: 1 },
  waste3WasteCodes:     { row: 29, col: 62, maxLen: 16 },
  // Line 4
  waste4HM:             { row: 31, col: 3,  maxLen: 1 },
  waste4Description:    { row: 31, col: 8,  maxLen: 32 },
  waste4ContainerNum:   { row: 31, col: 42, maxLen: 4 },
  waste4ContainerType:  { row: 31, col: 47, maxLen: 2 },
  waste4Qty:            { row: 31, col: 51, maxLen: 6 },
  waste4Unit:           { row: 31, col: 58, maxLen: 1 },
  waste4WasteCodes:     { row: 31, col: 62, maxLen: 16 },
  // Box 14 - Special Handling Instructions
  specialHandling:      { row: 34, col: 8,  maxLen: 65 },
  specialHandling2:     { row: 35, col: 8,  maxLen: 65 },
  // Box 15 - Generator Certification
  generatorPrintName:   { row: 39, col: 8,  maxLen: 30 },
  generatorDate:        { row: 39, col: 55, maxLen: 10 },
};

// Build raw text for dot matrix printer
function buildPrintData(manifest) {
  // Create a 66-line x 80-col grid of spaces
  const lines = [];
  for (let i = 0; i < 66; i++) {
    lines.push(' '.repeat(80));
  }

  // Place each field
  for (const [field, pos] of Object.entries(FORM_8700_MAP)) {
    let value = manifest[field] || '';
    if (typeof value !== 'string') value = String(value);
    value = value.substring(0, pos.maxLen);
    if (value) {
      const row = pos.row - 1; // 0-indexed
      const col = pos.col - 1;
      const line = lines[row];
      lines[row] = line.substring(0, col) + value + line.substring(col + value.length);
    }
  }

  return lines.join('\r\n');
}

// Print endpoint - returns plain text for dot matrix
app.post('/api/print/manifest/:id', (req, res) => {
  const data = loadData();
  const manifest = data.manifests.find(m => m.id === req.params.id);
  if (!manifest) return res.status(404).json({ error: 'Manifest not found' });

  const printData = buildPrintData(manifest);
  res.setHeader('Content-Type', 'text/plain');
  res.setHeader('Content-Disposition', `inline; filename="manifest-${manifest.manifestNum}.txt"`);
  res.send(printData);
});

// Raw print data endpoint (for direct printer output)
app.get('/api/print/raw/:id', (req, res) => {
  const data = loadData();
  const manifest = data.manifests.find(m => m.id === req.params.id);
  if (!manifest) return res.status(404).json({ error: 'Manifest not found' });

  const printData = buildPrintData(manifest);
  res.setHeader('Content-Type', 'application/octet-stream');
  res.setHeader('Content-Disposition', `attachment; filename="manifest-${manifest.manifestNum}.prn"`);
  res.send(printData);
});

// Start server
app.listen(PORT, () => {
  console.log(`Manifest Platform running on port ${PORT}`);
  console.log(`Open http://localhost:${PORT} in your browser`);
});

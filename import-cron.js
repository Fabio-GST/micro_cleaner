const cron = require('node-cron');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

// Configurações
const pastaImport = path.resolve(__dirname, '../imports');
const sipCodes = [200, 404, 487];

cron.schedule('0 2 * * *', async () => {
  console.log(`[${new Date().toISOString()}] Iniciando importação automática...`);

  const arquivos = fs.readdirSync(pastaImport).filter(f => f.endsWith('.csv'));

  for (const arquivo of arquivos) {
    const filePath = path.join(pastaImport, arquivo);

    let sipCode = sipCodes.find(code => arquivo.includes(code.toString()));
    if (!sipCode) {
      for (const code of sipCodes) {
        await processarArquivo(filePath, code);
      }
    } else {
      await processarArquivo(filePath, sipCode);
    }

    const pastaProcessados = path.join(pastaImport, 'processados');
    if (!fs.existsSync(pastaProcessados)) fs.mkdirSync(pastaProcessados);
    fs.renameSync(filePath, path.join(pastaProcessados, arquivo));
  }

  console.log(`[${new Date().toISOString()}] Importação automática finalizada.`);
});

async function processarArquivo(filePath, sipCode) {
  return new Promise((resolve, reject) => {
    console.log(`Processando ${filePath} com SIP ${sipCode}...`);
    const proc = spawn('node', [path.resolve(__dirname, 'main.js'), filePath, sipCode], { stdio: 'inherit' });

    proc.on('close', (code) => {
      if (code === 0) {
        console.log(`✔️  ${filePath} (SIP ${sipCode}) importado com sucesso.`);
        resolve();
      } else {
        console.error(`❌ Erro ao importar ${filePath} (SIP ${sipCode})`);
        reject();
      }
    });
  });
}
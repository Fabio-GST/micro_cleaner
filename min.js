const fs = require('fs');
const readline = require('readline');

const db = require('./db'); // Importar o módulo de banco de dados

async function getFileInfo(filePath) {
  try {
    const stats = fs.statSync(filePath);
    const fileSizeInBytes = stats.size;
    const fileSizeInMB = fileSizeInBytes / (1024 * 1024);

    console.log('\n=== TESTE - INFORMAÇÕES DO ARQUIVO ===');
    console.log('Arquivo:', filePath);
    console.log('Tamanho:', fileSizeInBytes.toLocaleString(), 'bytes');
    console.log('Tamanho:', fileSizeInMB.toFixed(2), 'MB');
    console.log('MODO TESTE: Processando apenas 100 linhas');
    console.log('=======================================\n');

    return stats;
  } catch (error) {
    console.error('Erro ao ler informações do arquivo:', error.message);
    return null;
  }
}

async function getCSVHeader(filePath) {
  const fileStream = fs.createReadStream(filePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    if (line.trim()) {
      const header = line.split(';');
      rl.close();
      fileStream.destroy();
      console.log('Primeira linha (amostra):', header);
      console.log('Formato: [data_hora, numero, duração/codigo]\n');
      return header;
    }
  }
  return null;
}


// Função para converter data_hora do formato "2025-04-15 07:30:01" para ISO
function convertToISO(dateTimeString) {
  try {
    // Formato esperado: "2025-04-15 07:30:01"
    const date = new Date(dateTimeString);
    
    // Verificar se a data é válida
    if (isNaN(date.getTime())) {
      console.warn(`Data inválida encontrada: ${dateTimeString}, usando data atual`);
      return new Date().toISOString();
    }
    
    return date.toISOString();
  } catch (error) {
    console.warn(`Erro ao converter data: ${dateTimeString}, usando data atual`);
    return new Date().toISOString();
  }
}

function processLines(lines, responseCode) {
  const calls200 = {};
  const calls404 = {};
  const calls487 = {};
  let processed = 0;

  lines.forEach(line => {
    const data = line.split(';');

    if (data.length < 2) {
      return;
    }

    const dateTime = data[0]; // Primeira coluna é a data_hora
    const number = data[1]; // Segunda coluna é o número
    processed++;

    // Converter data_hora para formato ISO
    const createdAt = convertToISO(dateTime);

    console.log(`Processando linha ${processed}: ${data.join(' | ')}`);
    console.log(`  Data/Hora original: ${dateTime} → ISO: ${createdAt}`);

    switch (responseCode) {
      case 200:
        // Para código 200, terceira coluna é a duração
        if (data.length >= 3) {
          const duration = parseInt(data[2]) || 0;
          if (!calls200[number] || duration > calls200[number].duration) {
            calls200[number] = {
              created_at: createdAt,
              updated_at: createdAt,
              number: number,
              duration: duration
            };
            console.log(`  → Calls200: ${number} com duração ${duration}s (${dateTime})`);
          } else {
            console.log(`  → Calls200: ${number} duração ${duration}s ignorada (menor que ${calls200[number].duration}s)`);
          }
        }
        break;

      case 404:
        // Para código 404, apenas registrar o número
        if (!calls404[number]) {
          calls404[number] = {
            created_at: createdAt,
            updated_at: createdAt,

            number: number
          };
          console.log(`  → Calls404: ${number} (${dateTime})`);
        } else {
          console.log(`  → Calls404: ${number} já existe, ignorando`);
        }
        break;

      case 487:
        // Para código 487, contar tentativas
        if (!calls487[number]) {
          calls487[number] = {
            created_at: createdAt,
            updated_at: createdAt,
            number: number,
            attemps: 1
          };
          console.log(`  → Calls487: ${number} - primeira tentativa (${dateTime})`);
        } else {
          calls487[number].attemps++;
          calls487[number].created_at = createdAt; // Atualizar para a tentativa mais recente
          console.log(`  → Calls487: ${number} - tentativa ${calls487[number].attemps} (${dateTime})`);
        }
        break;
    }
  });

  return { calls200, calls404, calls487, processed };
}

async function processCSVTest(filePath, responseCode) {
  if (!fs.existsSync(filePath)) {
    console.log('\nERRO: Arquivo não encontrado...\n');
    return;
  }

  // Mostrar informações do arquivo
  await getFileInfo(filePath);
  await getCSVHeader(filePath);

  const startTime = Date.now();
  const maxLines = 100; // Máximo de linhas para teste

  console.log(`Processando arquivo como código ${responseCode}`);
  console.log(`MODO TESTE: Processando apenas ${maxLines} linhas...\n`);

  // Configurar stream de leitura
  const fileStream = fs.createReadStream(filePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  let lineCount = 0;
  const lines = [];
  const consolidated = {
    calls200: {},
    calls404: {},
    calls487: {},
    total: 0
  };

  // Ler apenas as primeiras 100 linhas
  for await (const line of rl) {
    if (line.trim()) {
      lines.push(line);
      lineCount++;

      if (lineCount >= maxLines) {
        break;
      }
    }
  }

  rl.close();
  fileStream.destroy();

  console.log(`\nColetadas ${lineCount} linhas. Iniciando processamento...\n`);

  // Processar as linhas coletadas
  const result = processLines(lines, responseCode);

  // Consolidar resultados
  Object.assign(consolidated.calls200, result.calls200);
  Object.assign(consolidated.calls404, result.calls404);
  Object.assign(consolidated.calls487, result.calls487);
  consolidated.total = result.processed;

  console.log('\n=== RESULTADOS DO TESTE ===');
  console.log(`Total processado: ${consolidated.total} linhas`);
  console.log(`Calls200 encontrados: ${Object.keys(consolidated.calls200).length}`);
  console.log(`Calls404 encontrados: ${Object.keys(consolidated.calls404).length}`);
  console.log(`Calls487 encontrados: ${Object.keys(consolidated.calls487).length}`);

  // Mostrar alguns exemplos
  if (Object.keys(consolidated.calls200).length > 0) {
    console.log('\nExemplos Calls200:');
    Object.keys(consolidated.calls200).slice(0, 3).forEach(number => {
      const call = consolidated.calls200[number];
      console.log(`  ${number}: ${call.duration}s`);
    });
  }

  if (Object.keys(consolidated.calls404).length > 0) {
    console.log('\nExemplos Calls404:');
    Object.keys(consolidated.calls404).slice(0, 3).forEach(number => {
      console.log(`  ${number}`);
    });
  }

  if (Object.keys(consolidated.calls487).length > 0) {
    console.log('\nExemplos Calls487:');
    Object.keys(consolidated.calls487).slice(0, 3).forEach(number => {
      const call = consolidated.calls487[number];
      console.log(`  ${number}: ${call.attemps} tentativas`);
    });
  }

  console.log('\n=============================\n');

  // Salvar no banco (opcional para teste)
  const shouldSave = process.argv[4] === '--save';
  if (shouldSave) {
    console.log('Salvando dados de teste no banco...');
    await saveToDB(consolidated, responseCode);
  } else {
    console.log('Use --save para salvar os dados no banco de dados');
    console.log('Exemplo: node main_test.js 200.csv 200 --save');
  }

  const endTime = Date.now();
  console.log(`\nTeste concluído em ${(endTime - startTime) / 1000}s`);
}

async function saveToDB(data, responseCode) {
  console.log('\nSalvando dados de teste no banco...\n');

  const database = new db();
  await database.connect();

  switch(responseCode) {
    case 200:
      console.log(`Inserindo ${Object.keys(data.calls200).length} registros de calls200...`);
      await database.insertCalls200(data.calls200);
      break;
    case 404:
      console.log(`Inserindo ${Object.keys(data.calls404).length} registros de calls404...`);
      await database.insertCalls404(data.calls404);
      break;
    case 487:
      console.log(`Inserindo ${Object.keys(data.calls487).length} registros de calls487...`);
      await database.insertCalls487(data.calls487);
      break;
    default:
      console.log('Código de resposta não reconhecido:', responseCode);
  }

  await database.disconnect();
  console.log('Dados de teste salvos com sucesso!');
}

// Executar
const filePath = process.argv[2];
const responseCode = parseInt(process.argv[3]);

if (!filePath || !responseCode) {
  console.log('Uso: node main_test.js <caminho_do_arquivo.csv> <codigo_resposta> [--save]');
  console.log('Exemplo: node main_test.js 200.csv 200');
  console.log('Exemplo: node main_test.js 200.csv 200 --save');
  console.log('\nEste script processa apenas 100 linhas para teste');
  process.exit(1);
}

if (![200, 404, 487].includes(responseCode)) {
  console.log('Código de resposta deve ser: 200, 404 ou 487');
  process.exit(1);
}

processCSVTest(filePath, responseCode);
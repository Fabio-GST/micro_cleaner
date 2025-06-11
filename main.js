const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const fs = require('fs');
const os = require('os');
const readline = require('readline');

const db = require('./db');

if (isMainThread) {
  
  async function processFileSimple(filePath, responseCode) {
    if (!fs.existsSync(filePath)) {
      console.log('\nERRO: Arquivo não encontrado...\n');
      return;
    }

    const startTime = Date.now();
    const batchSize = 100000; // 100k linhas por batch
    const saveInterval = 1000000; // Salvar a cada 1M linhas
    
    console.log(`Processando arquivo: ${filePath}`);
    console.log(`Código de resposta: ${responseCode}`);
    console.log(`Batch size: ${batchSize.toLocaleString()}`);
    console.log(`Salvar a cada: ${saveInterval.toLocaleString()} linhas\n`);

    const fileStream = fs.createReadStream(filePath, {
      highWaterMark: 4 * 1024 * 1024 // 4MB buffer
    });
    
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });

    let lineCount = 0;
    let batch = [];
    const consolidated = {
      calls200: {},
      calls404: {},
      calls487: {},
      total: 0
    };

    // Worker único para simplicidade
    const worker = new Worker(__filename, { workerData: { isWorker: true } });
    let processingBatch = false;

    const processBatch = async (batchData) => {
      return new Promise((resolve, reject) => {
        if (processingBatch) {
          setTimeout(() => processBatch(batchData).then(resolve).catch(reject), 100);
          return;
        }

        processingBatch = true;

        const messageHandler = (result) => {
          consolidateResults(result, consolidated);
          processingBatch = false;
          worker.off('message', messageHandler);
          worker.off('error', errorHandler);
          resolve(result);
        };

        const errorHandler = (error) => {
          console.error('Erro no batch:', error);
          processingBatch = false;
          worker.off('message', messageHandler);
          worker.off('error', errorHandler);
          reject(error);
        };

        worker.once('message', messageHandler);
        worker.once('error', errorHandler);

        worker.postMessage({
          lines: batchData,
          responseCode: responseCode
        });
      });
    };

    // Processar arquivo linha por linha
    for await (const line of rl) {
      if (line.trim()) {
        batch.push(line);
        lineCount++;

        // Processar batch quando estiver cheio
        if (batch.length === batchSize) {
          const batchToProcess = [...batch];
          batch = [];

          try {
            await processBatch(batchToProcess);
          } catch (error) {
            console.error('Erro ao processar batch:', error.message);
          }

          // Log de progresso
          if (lineCount % 100000 === 0) {
            const elapsed = (Date.now() - startTime) / 1000;
            const linesPerSecond = lineCount / elapsed;
            const memUsage = process.memoryUsage();
            
            console.log(`Progresso: ${lineCount.toLocaleString()} linhas em ${elapsed.toFixed(1)}s`);
            console.log(`Velocidade: ${Math.round(linesPerSecond).toLocaleString()} linhas/s`);
            console.log(`Memória: ${Math.round(memUsage.heapUsed / 1024 / 1024)}MB`);
            console.log(`Registros únicos: calls487=${Object.keys(consolidated.calls487).length}`);
            console.log('---');
          }

          // Salvar no banco periodicamente
          if (lineCount % saveInterval === 0) {
            console.log(`\n💾 Salvando ${Object.keys(consolidated.calls487).length} registros no banco...`);
            await saveToDB(consolidated, responseCode);
            
            // Limpar memória após salvar
            consolidated.calls200 = {};
            consolidated.calls404 = {};
            consolidated.calls487 = {};
            
            if (global.gc) {
              global.gc();
            }
          }
        }
      }
    }

    // Processar último batch
    if (batch.length > 0) {
      await processBatch(batch);
    }

    // Fechar worker
    worker.terminate();

    // Salvar dados finais
    if (Object.keys(consolidated.calls487).length > 0) {
      console.log(`\n💾 Salvando dados finais: ${Object.keys(consolidated.calls487).length} registros...`);
      await saveToDB(consolidated, responseCode);
    }

    const endTime = Date.now();
    const totalTime = (endTime - startTime) / 1000;
    const linesPerSecond = lineCount / totalTime;
    
    console.log(`\n=== RESUMO FINAL ===`);
    console.log(`Tempo total: ${totalTime.toFixed(1)}s (${(totalTime / 60).toFixed(1)} min)`);
    console.log(`Velocidade média: ${Math.round(linesPerSecond).toLocaleString()} linhas/s`);
    console.log(`Total processado: ${lineCount.toLocaleString()} linhas`);
    console.log(`====================`);
  }

  function consolidateResults(result, consolidated) {
    for (const number in result.calls487) {
      if (!consolidated.calls487[number]) {
        consolidated.calls487[number] = result.calls487[number];
      } else {
        consolidated.calls487[number].attemps += result.calls487[number].attemps;
        consolidated.calls487[number].created_at = result.calls487[number].created_at;
        consolidated.calls487[number].updated_at = result.calls487[number].updated_at;
      }
    }
    consolidated.total += result.processed;
  }

  async function saveToDB(data, responseCode) {
    if (Object.keys(data.calls487).length === 0) return;

    const database = new db();
    await database.connect();
    
    console.log(`Inserindo ${Object.keys(data.calls487).length.toLocaleString()} registros de calls487...`);
    await database.insertCalls487(data.calls487);
    
    await database.disconnect();
    console.log('✅ Dados salvos com sucesso!');
  }

  // Executar
  const filePath = process.argv[2];
  const responseCode = parseInt(process.argv[3]);

  if (!filePath || !responseCode) {
    console.log('Uso: node main_simple.js <arquivo.csv> <codigo>');
    console.log('Exemplo: node main_simple.js ../487/487.csv 487');
    process.exit(1);
  }

  processFileSimple(filePath, responseCode);

} else {
  // Worker thread
  parentPort.on('message', ({ lines, responseCode }) => {
    const calls487 = {};
    let processed = 0;

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const data = line.split(';');

      if (data.length < 4) continue;

      const dateTime = data[0];
      const number = data[2];
      processed++;

      const createdAt = convertToISO(dateTime);
      const updatedAt = new Date().toISOString();

      if (!calls487[number]) {
        calls487[number] = {
          created_at: createdAt,
          updated_at: updatedAt,
          number: number,
          attemps: 1
        };
      } else {
        calls487[number].attemps++;
        calls487[number].created_at = createdAt;
        calls487[number].updated_at = updatedAt;
      }
    }

    parentPort.postMessage({ calls487, processed });
  });

  function convertToISO(dateTimeString) {
    try {
      const date = new Date(dateTimeString);
      if (isNaN(date.getTime())) {
        return new Date().toISOString();
      }
      return date.toISOString();
    } catch (error) {
      return new Date().toISOString();
    }
  }
}
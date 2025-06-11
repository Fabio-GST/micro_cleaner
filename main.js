const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const fs = require('fs');
const os = require('os');
const readline = require('readline');

const db = require('./db'); // Importar o módulo de banco de dados

if (isMainThread) {
  // Thread principal
  async function processCSVStreaming(filePath, sipCode) {
    if (!fs.existsSync(filePath)) {
      console.log('\nERRO: Arquivo não encontrado...\n');
      return;
    }

    console.log(`\n=== PROCESSAMENTO SIP CODE ${sipCode} ===`);
    console.log(`Arquivo: ${filePath}`);
    console.log(`Código SIP: ${sipCode}`);
    console.log(`=========================================\n`);

    const startTime = Date.now();
    const batchSize = 50000; // Aumentar batch size
    const numCPUs = Math.min(os.cpus().length, 8);

    console.log(`Iniciando processamento com ${numCPUs} threads...`);
    console.log(`Batch size: ${batchSize.toLocaleString()} linhas\n`);

    // Configurar stream de leitura
    const fileStream = fs.createReadStream(filePath, {
      highWaterMark: 2 * 1024 * 1024 // 2MB buffer
    });
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });

    let lineCount = 0;
    let batch = [];
    let batchNumber = 0;
    const consolidated = {
      calls200: {},
      calls404: {},
      calls487: {},
      total: 0
    };

    // Pool de workers pré-criados
    const workers = [];
    for (let i = 0; i < numCPUs; i++) {
      workers.push({
        worker: new Worker(__filename, { workerData: { isWorker: true } }),
        busy: false,
        id: i
      });
    }

    // Função para processar batch
    const processBatch = async (batchData, batchNum) => {
      return new Promise((resolve, reject) => {
        let availableWorker = null;
        let attempts = 0;
        
        const findWorker = () => {
          availableWorker = workers.find(w => !w.busy);
          if (!availableWorker && attempts < 50) {
            attempts++;
            setTimeout(findWorker, 100);
            return;
          }
          
          if (!availableWorker) {
            reject(new Error(`Nenhum worker disponível após ${attempts * 100}ms`));
            return;
          }

          availableWorker.busy = true;

          const messageHandler = (result) => {
            consolidateResults(result, consolidated);

            if (batchNum % 50 === 0) {
              console.log(`Batch ${batchNum} processado: ${result.processed} linhas`);
            }

            availableWorker.busy = false;
            availableWorker.worker.off('message', messageHandler);
            availableWorker.worker.off('error', errorHandler);
            resolve(result);
          };

          const errorHandler = (error) => {
            console.error(`Erro no batch ${batchNum}:`, error);
            availableWorker.busy = false;
            availableWorker.worker.off('message', messageHandler);
            availableWorker.worker.off('error', errorHandler);
            reject(error);
          };

          availableWorker.worker.once('message', messageHandler);
          availableWorker.worker.once('error', errorHandler);

          // Enviar dados com o código SIP
          availableWorker.worker.postMessage({
            lines: batchData,
            batchNumber: batchNum,
            sipCode: sipCode
          });
        };

        findWorker();
      });
    };

    // Array para controlar batches em processamento
    const activeBatches = [];
    const maxConcurrentBatches = 4;

    // Processar arquivo linha por linha
    for await (const line of rl) {
      if (line.trim()) {
        batch.push(line);
        lineCount++;

        // Quando batch estiver cheio, processar
        if (batch.length === batchSize) {
          batchNumber++;
          const batchToProcess = [...batch];
          batch = [];

          // Aguardar se há muitos batches em processamento
          while (activeBatches.length >= maxConcurrentBatches) {
            try {
              await Promise.race(activeBatches);
              
              // Remover batches concluídos
              for (let i = activeBatches.length - 1; i >= 0; i--) {
                try {
                  const result = await Promise.race([
                    activeBatches[i], 
                    new Promise(resolve => setTimeout(() => resolve('pending'), 10))
                  ]);
                  if (result !== 'pending') {
                    activeBatches.splice(i, 1);
                  }
                } catch (error) {
                  activeBatches.splice(i, 1);
                }
              }
            } catch (error) {
              console.warn('Erro no controle de batches:', error.message);
            }
          }

          const batchPromise = processBatch(batchToProcess, batchNumber);
          activeBatches.push(batchPromise);

          // Log de progresso
          if (lineCount % 500000 === 0) {
            const elapsed = (Date.now() - startTime) / 1000;
            const linesPerSecond = lineCount / elapsed;
            const memUsage = process.memoryUsage();
            
            console.log(`\n=== PROGRESSO ===`);
            console.log(`Linhas: ${lineCount.toLocaleString()} em ${elapsed.toFixed(1)}s`);
            console.log(`Velocidade: ${Math.round(linesPerSecond).toLocaleString()} linhas/s`);
            console.log(`Memória: ${Math.round(memUsage.heapUsed / 1024 / 1024)}MB`);
            
            // Mostrar contadores baseado no código SIP
            switch(sipCode) {
              case 200:
                console.log(`Calls200: ${Object.keys(consolidated.calls200).length.toLocaleString()}`);
                break;
              case 404:
                console.log(`Calls404: ${Object.keys(consolidated.calls404).length.toLocaleString()}`);
                break;
              case 487:
                console.log(`Calls487: ${Object.keys(consolidated.calls487).length.toLocaleString()}`);
                break;
            }
            console.log(`==================\n`);

            if (global.gc) {
              global.gc();
            }
          }
        }
      }
    }

    // Processar último batch se houver
    if (batch.length > 0) {
      batchNumber++;
      await processBatch(batch, batchNumber);
    }

    // Aguardar todos os batches pendentes
    await Promise.allSettled(activeBatches);

    // Fechar workers
    workers.forEach(w => {
      if (w.worker) {
        w.worker.terminate();
      }
    });

    // Salvar consolidado no banco
    console.log(`\nProcessamento de ${lineCount.toLocaleString()} linhas concluído. Salvando no banco...`);
    await saveToDB(consolidated, sipCode);

    const endTime = Date.now();
    const totalTime = (endTime - startTime) / 1000;
    const linesPerSecond = lineCount / totalTime;
    
    console.log(`\n=== RESUMO FINAL ===`);
    console.log(`Tempo total: ${totalTime.toFixed(1)}s (${(totalTime / 60).toFixed(1)} min)`);
    console.log(`Velocidade média: ${Math.round(linesPerSecond).toLocaleString()} linhas/s`);
    console.log(`Total processado: ${consolidated.total.toLocaleString()} registros`);
    
    // Mostrar apenas a tabela relevante
    switch(sipCode) {
      case 200:
        console.log(`Calls200 únicos: ${Object.keys(consolidated.calls200).length.toLocaleString()}`);
        break;
      case 404:
        console.log(`Calls404 únicos: ${Object.keys(consolidated.calls404).length.toLocaleString()}`);
        break;
      case 487:
        console.log(`Calls487 únicos: ${Object.keys(consolidated.calls487).length.toLocaleString()}`);
        break;
    }
    console.log(`====================`);
  }

  function consolidateResults(result, consolidated) {
    // Consolidar calls200
    for (const number in result.calls200) {
      if (!consolidated.calls200[number] ||
        result.calls200[number].duration > consolidated.calls200[number].duration) {
        consolidated.calls200[number] = result.calls200[number];
      }
    }

    // Consolidar calls404
    for (const number in result.calls404) {
      consolidated.calls404[number] = result.calls404[number];
    }

    // Consolidar calls487
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

  async function saveToDB(data, sipCode) {
    console.log('\nSalvando dados no banco de dados...\n');

    const database = new db();
    await database.connect();

    // Salvar apenas na tabela correspondente ao código SIP
    switch(sipCode) {
      case 200:
        if (Object.keys(data.calls200).length > 0) {
          console.log(`Inserindo ${Object.keys(data.calls200).length.toLocaleString()} registros de calls200...`);
          await database.insertCalls200(data.calls200);
        } else {
          console.log('Nenhum registro calls200 para inserir.');
        }
        break;
        
      case 404:
        if (Object.keys(data.calls404).length > 0) {
          console.log(`Inserindo ${Object.keys(data.calls404).length.toLocaleString()} registros de calls404...`);
          await database.insertCalls404(data.calls404);
        } else {
          console.log('Nenhum registro calls404 para inserir.');
        }
        break;
        
      case 487:
        if (Object.keys(data.calls487).length > 0) {
          console.log(`Inserindo ${Object.keys(data.calls487).length.toLocaleString()} registros de calls487...`);
          await database.insertCalls487(data.calls487);
        } else {
          console.log('Nenhum registro calls487 para inserir.');
        }
        break;
        
      default:
        console.log(`Código SIP ${sipCode} não reconhecido. Dados não salvos.`);
        break;
    }

    await database.disconnect();
    console.log('✅ Dados salvos com sucesso!');
  }

  // Executar
  const filePath = process.argv[2];
  const sipCode = parseInt(process.argv[3]);

  if (!filePath || !sipCode) {
    console.log('Uso: node main.js <caminho_do_arquivo.csv> <codigo_sip>');
    console.log('');
    console.log('Exemplos:');
    console.log('  node main.js ../200/dados.csv 200     # Para códigos 200');
    console.log('  node main.js ../404/dados.csv 404     # Para códigos 404');
    console.log('  node main.js ../487/487.csv 487       # Para códigos 487');
    console.log('');
    console.log('Códigos SIP suportados: 200, 404, 487');
    process.exit(1);
  }

  if (![200, 404, 487].includes(sipCode)) {
    console.log('❌ Código SIP deve ser: 200, 404 ou 487');
    process.exit(1);
  }

  processCSVStreaming(filePath, sipCode);

} else {
  // Worker thread - PROCESSAMENTO BASEADO NO CÓDIGO SIP
  parentPort.on('message', ({ lines, batchNumber, sipCode }) => {
    const result = processLines(lines, sipCode);
    parentPort.postMessage(result);
  });

  function processLines(lines, sipCode) {
    const calls200 = {};
    const calls404 = {};
    const calls487 = {};
    let processed = 0;

    lines.forEach(line => {
      const data = line.split(',');

      if (data.length < 3) {
        return;
      }

      const dateTime = data[0];     // Data/hora
      const durationOrType = data[2]; // Duração ou tipo
      const number = data[1];       // Número

      processed++;

      // Converter data_hora para formato ISO
      const createdAt = convertToISO(dateTime);
      const updatedAt = new Date().toISOString();

      // Processar baseado no código SIP especificado
      switch (sipCode) {
        case 200:
          // Para código 200, data[1] é a duração
          const duration = parseInt(durationOrType) || 0;
          if (!calls200[number] || duration > calls200[number].duration) {
            calls200[number] = {
              created_at: createdAt,
              updated_at: updatedAt,
              number: number,
              duration: duration
            };
          }
          break;

        case 404:
          // Para código 404, apenas registrar o número
          if (!calls404[number]) {
            calls404[number] = {
              created_at: createdAt,
              updated_at: updatedAt,
              number: number
            };
          }
          break;

        case 487:
          // Para código 487, contar tentativas
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
          break;
      }
    });

    return { calls200, calls404, calls487, processed };
  }

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
const mysql = require('mysql2/promise');

class DatabaseManager {
  constructor() {
    this.connection = null;
  }

  async connect() {
    require('dotenv').config();
    this.connection = await mysql.createConnection({
      host: process.env.DB_HOST,
      port: process.env.DB_PORT,
      user: process.env.DB_USERNAME,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_DATABASE,
      // Remover configurações inválidas que causam warnings
      connectTimeout: 60000
    });

    // Otimizações para inserções em massa
    await this.connection.execute('SET autocommit = 0');
    await this.connection.execute('SET unique_checks = 0');
    await this.connection.execute('SET foreign_key_checks = 0');
    await this.connection.execute('SET sql_log_bin = 0');
  }

  async insertCalls200(calls200Data) {
    if (Object.keys(calls200Data).length === 0) return;

    const values = Object.values(calls200Data).map(call => [
      call.created_at || new Date().toISOString(),
      call.updated_at || new Date().toISOString(),
      call.number || '',
      call.duration || 0
    ]);

    await this.insertInBatches('calls200s', ['created_at', 'updated_at', 'number', 'duration'], values, 5000);
  }

  async insertCalls404(calls404Data) {
    if (Object.keys(calls404Data).length === 0) return;

    const values = Object.values(calls404Data).map(call => [
      call.created_at || new Date().toISOString(),
      call.updated_at || new Date().toISOString(),
      call.number || ''
    ]);

    await this.insertInBatches('calls404s', ['created_at', 'updated_at', 'number'], values, 5000);
  }

  async insertCalls487(calls487Data) {
    if (Object.keys(calls487Data).length === 0) return;

    const values = Object.values(calls487Data).map(call => [
      call.created_at || new Date().toISOString(),
      call.updated_at || new Date().toISOString(),
      call.number || '',
      call.attemps || 1
    ]);

    await this.insertInBatches('calls487s', ['created_at', 'updated_at', 'number', 'attemps'], values, 5000);
  }

  async insertCalls48x(calls48xData) {
    if (Object.keys(calls48xData).length === 0) return;
    const values = Object.values(calls48xData).map(call => [
      call.created_at || new Date().toISOString(),
      call.updated_at || new Date().toISOString(),
      call.number || '',
    ]);

    await this.insertInBatches('calls48X', ['created_at', 'updated_at', 'number', ], values, 5000);
  }

  async insertInBatches(tableName, columns, values, batchSize = 5000) {
    const totalBatches = Math.ceil(values.length / batchSize);

    console.log(`Inserindo ${values.length} registros em ${totalBatches} lotes na tabela ${tableName}...`);

    // Validar dados antes da inserção
    const validValues = values.filter(row => {
      return row.every(value => value !== undefined && value !== null);
    });

    if (validValues.length !== values.length) {
      console.warn(`⚠️  ${values.length - validValues.length} registros removidos por conter valores inválidos`);
    }

    if (validValues.length === 0) {
      console.log('⚠️  Nenhum registro válido para inserir');
      return;
    }

    // Iniciar transação
    await this.connection.beginTransaction();

    try {
      for (let i = 0; i < validValues.length; i += batchSize) {
        const batch = validValues.slice(i, i + batchSize);
        const placeholders = batch.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
        const flatValues = batch.flat();

        const query = `INSERT IGNORE INTO ${tableName} (${columns.join(', ')}) VALUES ${placeholders}`;

        await this.connection.execute(query, flatValues);

        const currentBatch = Math.floor(i / batchSize) + 1;
        console.log(`Lote ${currentBatch}/${totalBatches} inserido (${batch.length} registros)`);

        // Commit a cada 10 lotes para evitar transações muito grandes
        if (currentBatch % 10 === 0) {
          await this.connection.commit();
          await this.connection.beginTransaction();
        }
      }

      // Commit final
      await this.connection.commit();
      console.log(`✅ Inserção em ${tableName} concluída com sucesso!`);

    } catch (error) {
      await this.connection.rollback();
      console.error(`❌ Erro na inserção em ${tableName}:`, error.message);
      throw error;
    }
  }

  async disconnect() {
    if (this.connection) {
      // Restaurar configurações padrão
      try {
        await this.connection.execute('SET autocommit = 1');
        await this.connection.execute('SET unique_checks = 1');
        await this.connection.execute('SET foreign_key_checks = 1');
        await this.connection.execute('SET sql_log_bin = 1');
      } catch (error) {
        console.warn('Aviso ao restaurar configurações:', error.message);
      }

      await this.connection.end();
      this.connection = null;
    }
  }

  async close() {
    await this.disconnect();
  }
}

module.exports = DatabaseManager;
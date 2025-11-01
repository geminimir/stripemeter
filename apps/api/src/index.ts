/**
 * Stripemeter API Server Entry Point
 */

import 'dotenv/config';
import { buildServer } from './server';
import { logger } from './utils/logger';

const start = async () => {
  try {
    const server = await buildServer();
    
    const port = parseInt(process.env.API_PORT || '3000', 10);
    const host = process.env.API_HOST || '0.0.0.0';
    
    await server.listen({ port, host });
    
    logger.info(`🚀 Stripemeter API server running at http://${host}:${port}`);
    logger.info(`📚 API documentation available at http://${host}:${port}/docs`);
    
    // Graceful shutdown
    const signals = ['SIGINT', 'SIGTERM'];
    signals.forEach((signal) => {
      process.on(signal, async () => {
        logger.info(`Received ${signal}, shutting down gracefully...`);
        await server.close();
        process.exit(0);
      });
    });
  } catch (error) {
    console.log('err', error);
    logger.error('Failed to start server:', error);
    process.exit(1);
  }
};

start();

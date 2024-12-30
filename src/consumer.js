const { Kafka } = require('kafkajs');
const fs = require('fs');

const kafka = new Kafka({
  clientId: 'cdp-consumer',
  brokers: ['192.168.1.3:9092'], 
});

const consumer = kafka.consumer({ groupId: 'cdp-group' });

const consumeMessages = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'cdp-events', fromBeginning: true });

  const batchSize = 100;
  const batches = {}; 

  const outputDir = './consumer_messages';
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir);
  }

  consumer.run({
    eachMessage: async ({ message }) => {
      const payload = JSON.parse(message.value.toString());
      const { consumer_id } = payload;

      if (!batches[consumer_id]) {
        batches[consumer_id] = [];
      }

      batches[consumer_id].push(payload);

      if (batches[consumer_id].length >= batchSize) {
        const filePath = `${outputDir}/${consumer_id}_messages.txt`;
        fs.appendFileSync(filePath, JSON.stringify(batches[consumer_id], null, 2) + '\n');
        console.log(`Written batch for consumer '${consumer_id}' to ${filePath}`);
        batches[consumer_id] = []; 
      }
    },
  });

  process.on('SIGINT', async () => {
    console.log('Shutting down gracefully...');
    for (const consumerId in batches) {
      if (batches[consumerId].length > 0) {
        const filePath = `${outputDir}/${consumerId}_messages.txt`;
        fs.appendFileSync(filePath, JSON.stringify(batches[consumerId], null, 2) + '\n');
        console.log(`Written remaining messages for consumer '${consumerId}' to ${filePath}`);
      }
    }
    await consumer.disconnect();
    process.exit(0);
  });
};

consumeMessages().catch(console.error);

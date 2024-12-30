const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'cdp-producer',
  brokers: ['192.168.1.3:9092'], 
});

const producer = kafka.producer();
const consumers = ['membr', 'fieldedge', 'marianatek', 'kos'];

const produceMessages = async () => {
  await producer.connect();

  const messages = [];

  
  consumers.forEach((consumer) => {
    for (let i = 0; i < 100; i++) {
      messages.push({
        value: JSON.stringify({
          consumer_id: consumer,
          message: `Message ${i + 1} for ${consumer}`,
          timestamp: new Date().toISOString(),
        }),
      });
    }
  });

  
  for (let i = messages.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1)); 
    [messages[i], messages[j]] = [messages[j], messages[i]]; 
  }

  
  await producer.send({
    topic: 'cdp-events',
    messages,
  });

  console.log('Randomized messages published successfully.');
  await producer.disconnect();
};

produceMessages().catch(console.error);

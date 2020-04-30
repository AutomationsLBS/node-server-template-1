const kafka = require('kafka-node');
const config = require('./configure.js');

const value = {
    v: '3.3.18',
    version: '1132',
    session_id: '1588162083138_41648d70-109d-412b-9032-2179e78e1b11',
    language_code: 'en',
    country_code: 'nl',
    timezone: 'Europe/Amsterdam',
    device_id: '41648d70-109d-412b-9032-2179e78e1b11',
    user_id: 212698505001102,
    advertising_id: '41648D70-109D-412B-9032-2179E78E1B11',
    app: 'com.picsart.studio',
    platform: 'apple',
    market: 'apple',
    event_type: 'video_music_action',
    timestamp: 1588162693455,
    experiments: [
    ],
    data: {
        editor_sid: 'ad04e025-4ebc-4545-b218-600c897baeca_1588162556241',
        action: 'try',
        overlay_session_id: 'ad04e025-4ebc-4545-b218-600c897baeca_1588162654258',
        result_source: 'epidemic',
        category: 'dance',
        music_id: 'youre_the_only_reason',
        music_title: 'you\'re the only reason',
        music_duration: 177,
        download_time: 1,
        music_is_free: false,
        is_subscribed: false
    }
};

const event = {
    value: JSON.stringify(value)
};

try {
    const { Producer, KafkaClient } = kafka;
    const client = new KafkaClient({
        kafkaHost: config.kafka_server
    });
    const producer = new Producer(client);
    const { kafka_topic } = config;

    const payloads = [
        {
            topic: kafka_topic,
            messages: [
                event.value
            ],
        }
    ];

    producer.on('ready',  () => {
        producer.send(payloads, (err, data) => {
            if (err) {
                console.log(`[kafka-producer -> ${kafka_topic}]: broker update failed`);
                console.log({ err });
            } else {
                console.log({ data });
                console.log(`[kafka-producer -> ${kafka_topic}]: broker update success`);
            }
        });
    });

    producer.on('error', (err) => {
        console.log({ err });
        console.log(`[kafka-producer -> ${kafka_topic}]: connection errored`);
        throw err;
    });
    console.log('end of try');
} catch (e) {
    console.log({ e });
}

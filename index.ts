import { connect, Channel, Connection } from "amqplib-as-promised";
import { Message } from 'amqplib'
import { Message as TransporterMessage, Transporter, ListenOptions, CallBackFunction, PublishOptions } from 'typescript-microservice'


export class AmqpTransporter implements Transporter {


    constructor(
        private push_channel: Channel,
        private listen_connection: Connection,
        private listen_default_channel: Channel
    ) { }

    static async init(url: string = process.env.AMQP_TRANSPORTER) {
        process.env.TSMS_DEBUG && console.log(`[TSMS_DEBUG] Connect to [${url}]`)
        const push_connection = await connect(url)
        const push_channel = await push_connection.createChannel() as any as Channel

        push_channel.on('error', msg => {
            console.error(msg.message)
        })

        const listen_connection = await connect(url) as any as Connection
        const listen_default_channel = await push_connection.createChannel() as any as Channel
        return new this(push_channel, listen_connection, listen_default_channel)
    }

    async publish(topic: string, data: Buffer, options: PublishOptions = {}) {
        process.env.TSMS_DEBUG && console.log(`[TSMS_DEBUG] Publish to topic [${topic}]`, JSON.stringify(options, null, 2))
        await this.push_channel.assertExchange(topic, 'topic', { autoDelete: true })
        await this.push_channel.publish(
            topic,
            options.route,
            data,
            { replyTo: options.reply_to, messageId: options.id }
        )
    }

    private async getChannel(options: ListenOptions) {
        if (options?.limit) {
            const channel = await this.listen_connection.createChannel()
            channel.prefetch(options.limit, false)
            return channel
        }
        return this.listen_default_channel
    }

    async listen(topic: string, cb: CallBackFunction, options: ListenOptions = {}) {

        const channel = await this.getChannel(options)
        const { queue } = await channel.assertQueue(options.fanout ? '' : `${topic}${options.route || ''}`, {
            autoDelete: true
        })
        process.env.TSMS_DEBUG && console.log(`[TSMS_DEBUG] Listen topic [${topic}] => bind to queue [${queue}]`, JSON.stringify(options, null, 2))
        await this.push_channel.assertExchange(topic, 'topic', { autoDelete: true })
        await channel.bindQueue(queue, topic, options.route || '#')
        await channel.consume(queue, async (msg: Message) => {
            const { content, properties: { timestamp, messageId, replyTo } } = msg
            const data: TransporterMessage = {
                content,
                created_time: timestamp,
                id: messageId,
                reply_to: replyTo,
                delivery_attempt: msg.properties.headers["x-death"]?.length || 0
            }
            await cb(data)
            channel.ack(msg)
        }, { noAck: false })
        return queue
    }

}
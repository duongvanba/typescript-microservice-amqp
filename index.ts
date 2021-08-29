import { Message, connect, Channel, Connection } from 'amqplib'
import { Message as TransporterMessage, Transporter, ListenOptions, CallBackFunction, PublishOptions } from 'typescript-microservice'
import { ReplaySubject, fromEvent, merge, firstValueFrom, mergeMap } from 'rxjs'


export class AmqpTransporter implements Transporter {

    private push_connection: Connection
    private listen_connection: Connection

    private push_channel: Channel
    private listen_default_channel: Channel
    private listeners = new ReplaySubject<{
        topic: string,
        cb: CallBackFunction,
        options: ListenOptions,
        queue_name: string
    }>(1000)

    private constructor(
        private url: string
    ) {

    }

    private setup_listeners() {

        return this
            .listeners
            .pipe(
                mergeMap(async ({ cb, options, topic, queue_name }) => {
                    const channel = await this.getChannel(options)
                    const { queue } = await channel.assertQueue(queue_name, {
                        autoDelete: true
                    })
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
                }, 1)
            )
            .subscribe()
    }

    private async init() {
        await new Promise(async (s, r) => {
            try {
                while (true) {

                    // Init connection
                    this.push_connection = await connect(this.url, { reconnectTimeInSeconds: 2, heartbeatIntervalInSeconds: 1 })
                    this.listen_connection = await connect(this.url, { reconnectTimeInSeconds: 2, heartbeatIntervalInSeconds: 1 })
                    this.push_channel = await this.push_connection.createChannel()
                    this.listen_default_channel = await this.push_connection.createChannel()

                    // Setup listeners
                    const subcription = await this.setup_listeners()

                    // Wait for error
                    s(1)
                    await firstValueFrom(
                        merge(
                            fromEvent(this.push_connection, 'error'),
                            fromEvent(this.listen_connection, 'error'),
                            fromEvent(this.push_channel, 'error'),
                            fromEvent(this.listen_default_channel, 'error')
                        )
                    )
                    console.log(`Error with rabbitmq, reconnect in 3s...`)
                    subcription.unsubscribe()
                    try {
                        this.listen_default_channel.close()
                        this.push_channel.close()
                        this.listen_connection.close()
                        this.push_connection.close()
                    } catch (e) {
                        console.log(e)
                    }
                    await new Promise(s => setTimeout(s, 3000))
                }
            } catch (e) {
                r(e)
            }
        })
    }

    static async init(url: string = process.env.AMQP_TRANSPORTER) {
        const instance = new this(url)
        await instance.init()
        return instance
    }

    async publish(topic: string, data: Buffer, options: PublishOptions = {}) {
        try {
            await this.push_channel.assertExchange(topic, 'topic', { autoDelete: true })
            await this.push_channel.publish(
                topic,
                options.route,
                data,
                { replyTo: options.reply_to, messageId: options.id }
            )
        } catch (e) {
            console.error(e)
        }
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
        const queue_name = options.fanout ? '' : `${topic}${options.route || ''}`
        this.listeners.next({ topic, cb, queue_name, options })
        return queue_name
    }

}
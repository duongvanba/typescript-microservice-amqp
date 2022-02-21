import { Message, connect, Channel, Connection } from 'amqplib'
import { Message as TransporterMessage, Transporter, ListenOptions, CallBackFunction, PublishOptions, sleep } from 'typescript-microservice'
import { fromEvent, firstValueFrom, Subject, Subscription, debounceTime, mergeMap, merge, of } from 'rxjs'

export type Callback = {
    topic: string,
    cb: CallBackFunction,
    options: ListenOptions,
    queue_name: string
}

export class AmqpTransporter implements Transporter {

    #push_connection: Connection
    #listen_connection: Connection

    #push_channel: Channel
    #listen_channels: Channel[]

    #callbacks: Callback[] = []
    #subscriptions: Subscription[] = []

    #$on_error = new Subject()

    private constructor(private readonly url: string) { }


    async #add_queue_listener({ cb, options, queue_name, topic }: Callback) {
        const channel = await this.#getChannel(options)
        const subscription = fromEvent(channel, 'error').subscribe(this.#$on_error)
        this.#subscriptions.push(subscription)
        const { queue } = await channel.assertQueue(queue_name, {
            autoDelete: true
        })
        await this.#push_channel.assertExchange(topic, 'topic', { autoDelete: true })
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
    }

    async #setup() {

        this.#push_connection = await connect(this.url, { reconnectTimeInSeconds: 2, heartbeatIntervalInSeconds: 1 })
        this.#listen_connection = await connect(this.url, { reconnectTimeInSeconds: 2, heartbeatIntervalInSeconds: 1 })
        this.#push_channel = await this.#push_connection.createChannel()
        this.#listen_channels = []

        for (const event of ['error', 'close']) {
            this.#subscriptions.push(fromEvent(this.#push_connection, event).subscribe(this.#$on_error))
            this.#subscriptions.push(fromEvent(this.#listen_connection, event).subscribe(this.#$on_error))
            this.#subscriptions.push(fromEvent(this.#push_channel, event).subscribe(this.#$on_error))
        }


        for (const cb of this.#callbacks) await this.#add_queue_listener(cb)

        return {
            unsubscribe: () => {
                try {
                    this.#subscriptions.map(s => s.unsubscribe())
                    this.#subscriptions.splice(0, this.#subscriptions.length)
                    this.#listen_channels.map(l => l.close())
                    this.#listen_channels.splice(0, this.#listen_channels.length)
                    this.#push_channel.close()
                    this.#listen_connection.close()
                    this.#push_connection.close()
                } catch (e) {
                    console.log({e})
                }
            }
        }
    }

    async #loop_and_keep_connect() {
        await new Promise(async success => {
            while (true) {
                try {
                    const subscription = await this.#setup()
                    success(1)
                    await firstValueFrom(this.#$on_error)
                    subscription.unsubscribe()
                } catch (e) {
                }
                console.log(`Error with rabbitmq, reconnect in 3s...`)
                await sleep(3000)
            }

        })
    }

    static async init(url: string = process.env.AMQP_TRANSPORTER) {
        const instance = new this(url)
        await instance.#loop_and_keep_connect()
        return instance
    }

    async publish(topic: string, data: Buffer, options: PublishOptions = {}) {
        try {
            await this.#push_channel.assertExchange(topic, 'topic', { autoDelete: true })
            await this.#push_channel.publish(
                topic,
                options.route,
                data,
                { replyTo: options.reply_to, messageId: options.id }
            )
        } catch (e) {
            console.error(e)
        }
    }

    async #getChannel(options: ListenOptions) {
        if (options?.limit || this.#listen_channels.length == 0) {
            const channel = await this.#listen_connection.createChannel()
            this.#listen_channels.push(channel)
            channel.prefetch(options.limit, false)
            return channel
        }
        return this.#listen_channels[0]
    }

    async listen(topic: string, cb: CallBackFunction, options: ListenOptions = {}) {
        const queue_name = options.fanout ? '' : `${topic}${options.route || ''}`
        const config: Callback = { cb, options, queue_name, topic }
        this.#callbacks.push(config)
        await this.#add_queue_listener(config)
        return queue_name
    }

}


setImmediate(async () => {
    setInterval(() => { }, 10000)
    const rabbitmq = await AmqpTransporter.init(`amqp://smmv3:c77uvFhAVuNtTf6Z@54.169.27.236:5672`)
    console.log('Connected')
    rabbitmq.listen('ahihi', data => console.log({ data }), { limit: 1 })
})


import { Message, connect, Channel, Connection } from 'amqplib'
import { Message as TransporterMessage, Transporter, ListenOptions, CallBackFunction, PublishOptions, sleep } from 'typescript-microservice'
import { fromEvent, firstValueFrom, Subject, Subscription } from 'rxjs'

export type Callback = {
    topic: string,
    cb: CallBackFunction,
    options: ListenOptions,
    queue_name: string
}

export class AmqpError extends Error {
    constructor(
        public code: string,
        public message: string
    ) {
        super(message)
    }
}


export class AmqpTransporter implements Transporter {

    #push_connection: Connection
    #listen_connection: Connection

    #push_channel: Channel
    #listen_channels: Channel[]

    #callbacks: Callback[] = []

    #$on_error = new Subject()

    private constructor(private readonly url: string) {
        process.env.DEBUG && this.#$on_error.subscribe(console.error)
    }

    async #listen({ queue_name, topic, options, cb }: Callback) {
        const channel = await this.#getListenChannel(options)
        const { queue } = await channel.assertQueue(queue_name, {
            autoDelete: true
        })
        await this.#push_channel.assertExchange(topic, 'topic', {
            autoDelete: true
        })
        await channel.bindQueue(queue, topic, options.route || '#')

        await channel.consume(queue, async (msg: Message) => {

            if (msg == null) {
                this.#$on_error.next('')
                return
            }
            try { channel.ack(msg) } catch (e) { }

            const { content, properties: { timestamp, messageId, replyTo, } } = msg
            const data: TransporterMessage = {
                content,
                created_time: timestamp,
                id: messageId,
                reply_to: replyTo,
                delivery_attempt: msg.properties.headers["x-death"]?.length || 0
            }
            await cb(data)
        }, { noAck: false })

        return channel
    }


    async #getListenChannel(options: ListenOptions) {
        if (options?.limit || this.#listen_channels.length == 0) {
            const channel = await this.#listen_connection.createChannel();
            this.#listen_channels.push(channel)
            options.limit && channel.prefetch(options.limit, false)
            return channel
        }
        return this.#listen_channels[0]
    }

    async #setup() {

        this.#push_connection = await connect(this.url, { reconnectTimeInSeconds: 2, heartbeatIntervalInSeconds: 1 })
        this.#listen_connection = await connect(this.url, { reconnectTimeInSeconds: 2, heartbeatIntervalInSeconds: 1 })
        this.#push_channel = await this.#push_connection.createChannel()

        this.#listen_channels = []
        const subscriptions = []

        for (const event of ['error', 'close']) {
            subscriptions.push(fromEvent(this.#push_connection, event).subscribe(this.#$on_error))
            subscriptions.push(fromEvent(this.#listen_connection, event).subscribe(this.#$on_error))
            subscriptions.push(fromEvent(this.#push_channel, event).subscribe(this.#$on_error))
        }


        for (const cb of this.#callbacks) {
            const channel = await this.#listen(cb)
            for (const event of ['error', 'close']) {
                subscriptions.push(fromEvent(channel, event).subscribe(this.#$on_error))
            }
        }

        return {
            clear: async () => {
                await subscriptions.map(s => s.unsubscribe())
                try { await this.#listen_connection.close() } catch (e) { }
                try { await this.#push_connection.close() } catch (e) { }
            }
        }
    }


    static async init(url: string = process.env.AMQP_TRANSPORTER) {
        const amqp = new this(url)

        // Infiniti loop
        await new Promise(async success => {
            while (true) {
                try {
                    const loop = await amqp.#setup()
                    success(1)
                    await firstValueFrom(amqp.#$on_error)
                    await loop.clear()
                } catch (e) { }
                console.error(`Error with rabbitmq, reconnect in 1s...`)
                await sleep(1000)
            }

        })
        return amqp
    }

    async publish(topic: string, data: Buffer, options: PublishOptions = {}) {
        try {
            await this.#push_channel.assertExchange(topic, 'topic', { autoDelete: true, })
            await this.#push_channel.publish(
                topic,
                options.route,
                data,
                {
                    replyTo: options.reply_to,
                    messageId: options.id,

                }
            )
        } catch (e) {
            process.env.DEBUG && console.error(e)
        }
    }



    listen(topic: string, cb: CallBackFunction, options: ListenOptions = {}) {
        const queue_name = options.fanout ? '' : `${topic}${options.route || ''}`
        const config: Callback = { cb, options, queue_name, topic }
        this.#callbacks.push(config)
        this.#listen(config)
        return {
            unsubscribe: () => { }
        } as any
    }

} 

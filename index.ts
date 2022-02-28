import { Message, connect, Channel, Connection } from 'amqplib'
import { Message as TransporterMessage, Transporter, ListenOptions, CallBackFunction, PublishOptions, sleep } from 'typescript-microservice'
import { fromEvent, firstValueFrom, Subject } from 'rxjs'

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

    private constructor(private readonly url: string) { }


    async #listen({ queue_name, topic, options, cb }: Callback) {
        const channel = await this.#getListenChannel(options)
        const { queue } = await channel.assertQueue(queue_name, {
            autoDelete: true,
        })
        await this.#push_channel.assertExchange(topic, 'topic', { autoDelete: true, })
        await channel.bindQueue(queue, topic, options.route || '#')

        await channel.consume(queue, async (msg: Message) => {

            if (msg == null) {
                this.#$on_error.next('')
                return
            }
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

        console.log(`Rabbitmq server connected`)

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
            unsubscribe: async () => {
                await subscriptions.map(s => s.unsubscribe())
                try { await this.#listen_connection.close() } catch (e) { }
                try { await this.#push_connection.close() } catch (e) { }
            }
        }
    }

    async #start() {
        await new Promise(async success => {
            while (true) {
                try {
                    const subscription = await this.#setup()
                    success(1)
                    await firstValueFrom(this.#$on_error)
                    await subscription.unsubscribe()
                } catch (e) {
                }
                console.log(`Error with rabbitmq, reconnect in 1s...`)
                await sleep(1000)
            }

        })
    }

    static async init(url: string = process.env.AMQP_TRANSPORTER) {
        const instance = new this(url)
        await instance.#start()
        return instance
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
            console.error(e)
        }
    }



    async listen(topic: string, cb: CallBackFunction, options: ListenOptions = {}) {
        const queue_name = options.fanout ? '' : `${topic}${options.route || ''}`
        const config: Callback = { cb, options, queue_name, topic }
        this.#callbacks.push(config)
        await this.#listen(config)
        return queue_name
    }

}


setImmediate(async () => {
    setInterval(() => { }, 10000)
    const rabbitmq = await AmqpTransporter.init(`amqp://smmv3:c77uvFhAVuNtTf6Z@54.169.27.236:5672`)
    console.log('Connected')
    await rabbitmq.listen('ahihi1', async data => {
        console.log('data received')
        await new Promise(s => setTimeout(s, 3000))
        console.log({data})
        return 1
    }, { limit: 1 })

    console.log('Sending')
    await rabbitmq.publish('ahihi1', Buffer.from('123'))
    console.log('Sent')
})


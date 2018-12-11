package com.github.hirokazumiyaji.emitterprocessor

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.EmitterProcessor
import java.time.Instant

@SpringBootApplication
class Application

fun main(args: Array<String>) {
    runApplication<Application>(*args)
}

@Configuration
class Configuration {
    @Bean
    fun queue(): Queue {
        return Queue()
    }
}

class Queue {
    private val emitter = EmitterProcessor.create<String>()
    private val sink = emitter.sink()

    fun enqueue(data: String) {
        sink.next(data)
    }

    fun subscribe(f: (data: String) -> Unit) {
        emitter.subscribe {
            f(it)
        }
    }
}

@Service
class Producer(private val queue: Queue) {
    fun send(data: String) {
        queue.enqueue(data)
    }
}

@Service
class Consumer(private val queue: Queue) {
    init {
        subscribe()
    }

    private final fun subscribe() {
        println("start subscribe")
        queue.subscribe { println("${Instant.now()}: $it") }
    }
}

data class Request(val ids: List<String>)

@RestController
@RequestMapping("")
class Controller(private val producer: Producer) {
    @PostMapping("")
    fun post(@RequestBody body: Request) {
        body.ids.forEach { producer.send(it) }
    }
}
package com.assignment.reportservice.config;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.assignment.reportservice.util.Constants;


@Configuration
public class RabbitMQConfig {
	
	@Bean
	public Queue queue() {
		return new Queue(Constants.ACCOUNT_STATE_CHANGE_QUEUE);
	}
    
	@Bean
	public TopicExchange topicExchange() {
		return new TopicExchange(com.assignment.reportservice.util.Constants.TOPIC_EXCHANGE);
		
	}
	
	@Bean
	public Binding binding(Queue queue,TopicExchange topicExchange) {
		return BindingBuilder
				.bind(queue)
				.to(topicExchange)
				.with(Constants.ROUTING_KEY);
	}
	
	@Bean
	public MessageConverter messageConverter() {
		return new Jackson2JsonMessageConverter();
	}
	
	@Bean
	public AmqpTemplate template(ConnectionFactory connectionFactory) {
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setMessageConverter(messageConverter());
		return template;
	}
}

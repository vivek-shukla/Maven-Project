package com.assignment.reportservice.consumer;

import java.time.Instant;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.assignment.reportservice.domain.AccountStateChange;
import com.assignment.reportservice.domain.Tag;
import com.assignment.reportservice.util.Constants;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

@Component
public class AccountStateChangeListener {
	
	@Autowired 
	private InfluxDBClient client;
	
	@RabbitListener(queues= Constants.ACCOUNT_STATE_CHANGE_QUEUE)
	public void listener(AccountStateChange accountStateChange) {
		System.out.println(accountStateChange);
		String tag = accountStateChange.getTransactionId() == null ? Tag.ACCOUNT_CREATION.name() : Tag.TRANSACTION.name();
		Point point = Point
				  .measurement(Constants.MEASUREMENT)
				  .addTag(Constants.STATE_CHANGE_TAG, tag)
				  .addField(Constants.CUSTOMER_ID, accountStateChange.getCustomerId())
				  .addField(Constants.ACCOUNT_ID, accountStateChange.getAccountId())
				  .addField(Constants.COUNTRY, accountStateChange.getCountry())
				  .addField(Constants.BALANCE, accountStateChange.getBalance())
				  .addField(Constants.CURRENCY_WALLET_STATE, accountStateChange.getCurrencyWalletStates().toString())
				  .addField(Constants.TRANSACTION_ID, accountStateChange.getTransactionId())
				  .addField(Constants.TRANSACTION_AMOUNT, accountStateChange.getTransactionAmount())
				  .addField(Constants.TRANSACTION_CURRENCY_ID, accountStateChange.getTransactionCurrencyId())
				  .addField(Constants.TRANSACTION_EXCHANGE_RATE, accountStateChange.getTransactionExchangeRate())
				  .addField(Constants.DIRECTION, accountStateChange.getDirection())
				  .addField(Constants.DESCRIPTION, accountStateChange.getDescription())
				  .addField(Constants.AVAILABLE_AMOUNT, accountStateChange.getAvailableAmount())
				  .time(Instant.now(), WritePrecision.NS);
		try(WriteApi writeApi = client.getWriteApi()) {
			writeApi.writePoint(Constants.BUCKET, Constants.ORG, point);
		}
	}

}

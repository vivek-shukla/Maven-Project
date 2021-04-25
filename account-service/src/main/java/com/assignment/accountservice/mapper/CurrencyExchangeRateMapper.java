package com.assignment.accountservice.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;

import org.springframework.cache.annotation.Cacheable;

import com.assignment.accountservice.util.Constants;
import com.assignment.accountservice.domain.CurrencyExchangeRate;

@Mapper
public interface CurrencyExchangeRateMapper {
	
	@Cacheable(Constants.CURRENCY_EXCHANGE_RATE_CACHE)
	@Select("select * from currency_exchange_rate")
	@Results({
	    @Result(property = "currencyId", column = "currency_id"),
	    @Result(property = "currencyCode", column = "currency_code"),
	    @Result(property = "exchangeRate", column = "exchange_rate"),
	    @Result(property = "inverseExchangeRate", column = "inverse_exchange_rate")
	})
	public List<CurrencyExchangeRate> findAll();

}

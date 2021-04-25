package com.assignment.accountservice.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import com.assignment.accountservice.domain.CurrencyWallet;

@Mapper
public interface CurrencyWallletMapper {
	
	@Insert("INSERT INTO currency_wallet(account_id,currency_id) " +
	        "VALUES (#{accountId}, #{currencyId})")
		public int insert(CurrencyWallet currencyWallet);
    
	@Select("SELECT currency_id FROM currency_wallet where account_id = #{accountId}")
	public List<Long> getCurrenciesforAccount(Long accountId);
	
	@Select("SELECT currency_code FROM currency_wallet INNER JOIN currency_exchange_rate ON currency_wallet.account_id = #{accountId} AND currency_wallet.currency_id = currency_exchange_rate.currency_id")
	public List<String> getCurrencyCodesforAccount(Long accountId);

}

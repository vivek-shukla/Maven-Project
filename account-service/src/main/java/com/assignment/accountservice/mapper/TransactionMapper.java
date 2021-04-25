package com.assignment.accountservice.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import com.assignment.accountservice.domain.Transaction;

@Mapper
public interface TransactionMapper {
	
	@Insert("INSERT INTO transaction(transaction_id,account_id,amount,currency_id,transaction_exchange_rate,direction,description,available_balance)" +
	 "VALUES (#{transactionId}, #{accountId}, #{amount},#{currencyId},#{transactionExchangeRate},#{direction},#{description},#{availableAmount})")
	public int insert(Transaction transaction);
    
	@Select("SELECT * FROM transaction WHERE transaction_id = #{transactionId}")
    @Results(value = {
         @Result(property="transactionId", column="transaction_id"),
         @Result(property="accountId", column="account_id"),
         @Result(property="amount",column="amount"),
         @Result(property="currencyId",column="currency_id"),
         @Result(property="transactionExchangeRate",column="transaction_exchange_rate"),
         @Result(property="direction",column="direction"),
         @Result(property="description",column="description"),
         @Result(property="availableAmount",column="available_balance")
    })
	public Transaction findByTransactionId(String transactionId);
    
	@Select("SELECT * FROM transaction WHERE account_id = #{accountId}")
    @Results(value = {
         @Result(property="transactionId", column="transaction_id"),
         @Result(property="accountId", column="account_id"),
         @Result(property="amount",column="amount"),
         @Result(property="currencyId",column="currency_id"),
         @Result(property="transactionExchangeRate",column="transaction_exchange_rate"),
         @Result(property="direction",column="direction"),
         @Result(property="description",column="description"),
         @Result(property="availableAmount",column="available_balance")
    })
	public List<Transaction> findByAccountId(Long accountId);

}

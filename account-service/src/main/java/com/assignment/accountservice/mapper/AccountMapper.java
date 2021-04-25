package com.assignment.accountservice.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import com.assignment.accountservice.domain.Account;

@Mapper
public interface AccountMapper {
	
	@Insert("INSERT INTO accounts(customer_id,country,balance) VALUES (#{customerId}, #{country}, #{balance})")
	public int insert(Account account);
    
	@Select("SELECT account_id FROM accounts WHERE customer_id = #{customerId}")
	public Long findByCustomerId(String customerId);
	
	@Select("SELECT account_id, customer_id, balance FROM accounts WHERE account_id = #{accountId}")
    @Results(value = {
         @Result(property="accountId", column="account_id"),
         @Result(property="customerId", column="customer_id"),
         @Result(property="balance",column="balance"),
         @Result(property="country",column="country")
    })  
	public Account findByAccountId(Long accountId);
    
	@Update("UPDATE accounts SET balance= #{balance} where account_id = #{accountId}")
	public void updateAccount(Account account);
	
	

}

package com.ztwu.bigdata.demo.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JdbcSinkDemo {
	public static void main(String[] args) {
		String sql = "insert into books (id, title, author, price, qty) values (?,?,?,?,?)";
		JdbcStatementBuilder<EtityDemo> jdbcStatementBuilder = new JdbcStatementBuilder<EtityDemo>() {
			@Override
			public void accept(PreparedStatement ps, EtityDemo t) throws SQLException {
				ps.setInt(1, t.id);
				ps.setString(2, t.title);
				ps.setString(3, t.author);
				ps.setDouble(4, t.price);
				ps.setInt(5, t.qty);
			}
		};
		JdbcConnectionOptions jdbcConnectionOptions = new
				JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl("")
				.withDriverName("")
				.withUsername("")
				.withPassword("")
				.build();
		JdbcSink.sink(
				sql,
				jdbcStatementBuilder,
				jdbcConnectionOptions);
	}
}

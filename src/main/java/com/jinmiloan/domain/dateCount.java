package com.jinmiloan.domain;

public class dateCount {
	private String time;
	private int sum_userid;
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public int getSum_userid() {
		return sum_userid;
	}
	public void setSum_userid(int sum_userid) {
		this.sum_userid = sum_userid;
	}
	public dateCount(String time, int sum_userid) {
		super();
		this.time = time;
		this.sum_userid = sum_userid;
	}
	public dateCount() {
		super();
		// TODO Auto-generated constructor stub
	}
	@Override
	public String toString() {
		return "dateCount [time=" + time + ", sum_userid=" + sum_userid + "]";
	}
	
}

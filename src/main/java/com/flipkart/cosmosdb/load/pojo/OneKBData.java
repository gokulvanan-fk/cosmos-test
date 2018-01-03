package com.flipkart.cosmosdb.load.pojo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.cosmosdb.load.data.OneKBLoad;
import com.flipkart.cosmosdb.load.utils.RandomUtil;
import com.microsoft.azure.documentdb.Document;

public class OneKBData {

	public String id;
	public String oid; // partition key
	public String name;
	public String accountId;
	public String blob;
	
	public OneKBData(String id){
		this.id = id;
		this.oid = id;
		this.name = OneKBLoad.generateRandomName();
		this.accountId =  OneKBLoad.generateAccountId(id);
		this.blob = "sasafdsfasdfasdfaasfdsafadsasdfsdafadsfdsfjklasdjfkldjklqwjekrjewqklrjeqwklrjewqklrjewqklrjeqklwrjqwklrjweklqjrwkleqjrkwejrkqwjrkwqjerklwejrklqwjerklewjrklewjrklewqjrklqwejrjfladksjflkdsjsjdfksajdfkladsjfkldsjfklakaaaaaaaaasjfdksfjadklsfjkldsjfklewjfqiowejfeklwflksfklasdfkdsfjsdakljfklsadjfaklsdjfklsadfjsadklfklsadjfdklsjfkldsjfklsdjfklasdjflksadjfsdjsdlkfjklsjfasfjdskwioerewfdslaljdksfjdskljfdsjfklsdjktestsfsdfsdfdsfdsfdsfdsdfdsfdsfadsfasdfds";	
		this.blob+=blob;
	}

	public OneKBData(Document response) {
		this.id = response.getId();
		this.oid = response.getString("oid");
		this.name = response.getString("name");
		this.accountId = response.getString("accountId");
		this.blob = response.getString("blob");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((accountId == null) ? 0 : accountId.hashCode());
		result = prime * result + ((blob == null) ? 0 : blob.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((oid == null) ? 0 : oid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OneKBData other = (OneKBData) obj;
		if (accountId == null) {
			if (other.accountId != null)
				return false;
		} else if (!accountId.equals(other.accountId))
			return false;
		if (blob == null) {
			if (other.blob != null)
				return false;
		} else if (!blob.equals(other.blob))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (oid == null) {
			if (other.oid != null)
				return false;
		} else if (!oid.equals(other.oid))
			return false;
		return true;
	}
	
	@Override
	public String toString(){
		return id+","+accountId;
	}
	
	public static void main(String[] args) throws Exception{
		OneKBData data = new OneKBData(RandomUtil.INSTANCE.randomKey());
		ObjectMapper mapper = new ObjectMapper();
		byte[] bytes = mapper.writeValueAsBytes(data);
		System.out.println(bytes.length);
		
	}
	
}

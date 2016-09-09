package bamboo.trove.common.xml;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.bind.annotation.adapters.XmlAdapter;


public class DateAdapter extends XmlAdapter<String, Date>{
	private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	
	@Override
	public Date unmarshal(String v) throws Exception{
		synchronized (format){
			return format.parse(v);
		}
	}

	@Override
	public String marshal(Date v) throws Exception{
		synchronized (format){
			return format.format(v);
		}
	}

}

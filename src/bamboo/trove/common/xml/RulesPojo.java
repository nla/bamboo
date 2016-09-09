package bamboo.trove.common.xml;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="list")
@XmlAccessorType(XmlAccessType.FIELD)
public class RulesPojo{
	@XmlElement(name="rule")
	private List<RulePojo> rules = null;

	public List<RulePojo> getRules(){
		return rules;
	}
	public void setRules(List<RulePojo> rules){
		this.rules = rules;
	}
}

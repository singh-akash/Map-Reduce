package neu.mr.scajoop.io;

public class Text {
	private String text;

	public Text(Object text) {
		this.text = String.valueOf(text);
	}
	
	public String get() {
		return text;
	}
	
	public void set(String text) {
		this.text = text;
	}
	
	public String toString() {
		return text;
	}
}

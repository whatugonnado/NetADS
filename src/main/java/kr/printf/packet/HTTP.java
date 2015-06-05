package kr.printf.packet;

public class HTTP implements Protocol {
	public String src_ip;
	public String dest_ip;
	public String method;
	public String data;
	public String host;
	public String path;
	public Ethernet eth;
	public double ts;
}

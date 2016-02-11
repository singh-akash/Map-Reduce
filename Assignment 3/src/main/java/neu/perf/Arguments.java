package neu.perf;
import java.util.Arrays;

/**
 * Simple command line argument processing
 * 
 * @author Jan Vitek
 */
class Args {
  String[] args;

  Args(String[] args) {
    this.args = args;
  }

  boolean getFlag(String flag) {
    boolean found = false;
    flag = "-" + flag;
    for (int i = 0; i < args.length; i++)
      if (found = args[i].equals(flag)) {
        args[i] = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);
        break;
      }
    return found;
  }

  String getOpt(String opt) {
    opt = "-" + opt + "=";
    for (int i = 0; i < args.length; i++)
      if (args[i].startsWith(opt)) {
        String value = args[i].replaceAll(opt, "");
        args[i] = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);
        return value;
      }
    return null;
  }

  String getOpt(String opt, String defaultVal) {
    String res = getOpt(opt);
    return (res == null) ? defaultVal : res;
  }

  int length() {
    return args.length;
  }

  String[] get() {
    return args;
  }

  public String toString() {
    String res = "";
    for (String s : args)
      res += s + " ";
    return res;
  }
}
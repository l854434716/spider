import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Created by luozhi on 2017/6/1.
 */
public class DeffedTest {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {


        System.out.println("张三向李四借钱");
        long l = lend().joinUninterruptibly();

        System.out.println("张三向李四借钱" + l);
        System.out.println("张三开始打牌");
    }

    public static Deferred<Long> lend() {
        System.out.println("李四说回家看看家里还有多少钱");

        class GetCB implements Callback<Long, Long> {

            public Long call(Long arg) throws Exception {
                return arg;
            }
        }

        return check().addCallback(new GetCB());
    }

    public static Deferred<Long> check() {
        System.out.println("李四在家查看还有多少钱");
        try {
            Thread.sleep(10*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Deferred.fromResult(new Long(1000));
    }
}

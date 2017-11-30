import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Created by luozhi on 2017/6/5.
 */
public class Deff {

    private volatile int state=1;

    private Object result;

    private static final AtomicIntegerFieldUpdater<Deff> stateUpdater =
            AtomicIntegerFieldUpdater.newUpdater(Deff.class, "state");


    public  boolean casState(final int cmp, final int val) {
        return stateUpdater.compareAndSet(this, cmp, val);
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public  void  doLock(){

        synchronized (this){
             System.out.println(Thread.currentThread().getId()+"lock" + this.toString());

            try {
                Thread.sleep(1000*30);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println(Thread.currentThread().getId()+"unlock" + this.toString());
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("state", state)
                .append("result", result)
                .toString();
    }




    public  static  void  main(String [] args){

       final Deff deff=new Deff();


        Thread thread= new Thread(){
            @Override
            public void run() {
                deff.doLock();
            }
        };


        thread.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        deff.casState(1,2);

        deff.setResult(new Object());

        System.out.println(Thread.currentThread().getId()+"main" + deff.toString());

    }
}

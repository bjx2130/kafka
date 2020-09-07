package com.sino.timer;

public class Main {

    public static void main(String[] args) {
        Timer timer = new Timer(1, 10);
        System.out.println("start\t" + System.currentTimeMillis());
        timer.addTask(new TimerTask(3000, () -> {
            System.out.println("task\t" + "测试任务执行");
        }));
        System.out.println("stop\t" + System.currentTimeMillis());
    }
}

package com.wbbigdata.test;

import com.wbbigdata.service.HelloService;
import com.wbbigdata.service.impl.HelloServiceImpl;

public class HelloTest {

    public static void main(String[] args) {

        HelloServiceImpl helloService = new HelloServiceImpl();
        helloService.finadAll();
    }
}

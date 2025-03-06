package com.app.kafka;

import com.github.javafaker.Faker;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.stream.IntStream;

public class PizzaMessage {
    // 피자 메뉴를 설정. getRandomValueFromList()에서 임의의 피자명을 출력하는 데 사용.
    private static final List<String> pizzaNames = List.of("Potato Pizza", "Cheese Pizza",
            "Cheese Garlic Pizza", "Super Supreme", "Peperoni");
//    private static final List<String> pizzaNames = List.of("고구마 피자", "치즈 피자",
//            "치즈 갈릭 피자", "슈퍼 슈프림", "페페로니 피자");

    // 피자 가게명을 설정. getRandomValueFromList()에서 임의의 피자 가게명을 출력하는데 사용.
    private static final List<String> pizzaShop = List.of("A001", "B001", "C001",
            "D001", "E001", "F001", "G001", "H001", "I001", "J001", "K001", "L001", "M001", "N001",
            "O001", "P001", "Q001");

    public PizzaMessage() {
    }

    // random한 피자 메시지를 생성하고, 피자가게 명을 key로 나머지 정보를 value로 하여 Hashmap을 생성하여 반환
    public HashMap<String, String> produceMsg(Faker faker, Random random, int id) {
        String shopId = getRandomValueFromList(pizzaShop, random);
        String pizzaName = getRandomValueFromList(pizzaNames, random);

        String ordId = "ord" + id;
        String customerName = faker.name().fullName();
        String phoneNumber = faker.phoneNumber().phoneNumber();
        String address = faker.address().fullAddress();
        LocalDateTime now = LocalDateTime.now();
        String message = String.format("order_id:%s, shop:%s, pizza_name:%s, customer_name:%s, phone_number:%s, address:%s, time:%s"
                , ordId, shopId, pizzaName, customerName, phoneNumber, address
                , now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.KOREAN)));
        HashMap<String, String> messageMap = new HashMap<>();
        messageMap.put("key", shopId);
        messageMap.put("message", message);

        return messageMap;
    }

    private String getRandomValueFromList(List<String> pizzaShops, Random random) {
        int index = random.nextInt(pizzaShops.size());

        return pizzaShops.get(index);
    }

    public static void main(String[] args) {
        PizzaMessage pizzaMessage = new PizzaMessage();
        long seed = 2025;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        IntStream.range(0, 60).forEach(i -> {
            HashMap<String, String> message = pizzaMessage.produceMsg(faker, random, i);
            System.out.println("key:"+ message.get("key") + " message:" + message.get("message"));
        });
    }
}

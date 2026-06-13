import os  # Модуль для работы с файловой системой ОС (создание папок)
import random  # Модуль для генерации случайных чисел и выбора элементов
import pandas as pd  # Библиотека Pandas для формирования и манипулирования датафреймами
from faker import Faker  # Библиотека Faker для генерации фейковых персональных данных (ФИО, телефоны)
from datetime import datetime, timedelta  # Модули для работы с временными интервалами

def generate_synthetic_data():
    # Сигнализируем в консоль о начале работы процесса генерации датасета
    print("Инициализация генерации данных для Интернет-магазина...")
    
    # Инициализируем генератор Faker с русской локалью для создания реалистичных ФИО
    fake = Faker('ru_RU')
    
    # Жестко фиксируем сиды (Seed), чтобы при каждом перезапуске Airflow генерировались идентичные строки
    Faker.seed(42)      
    random.seed(42)

    # Внутренний генератор 500 уникальных покупателей интернет-магазина
    def generate_customers(n):
        customers = []  # Создаем пустой список для аккумулирования словарей
        for i in range(1, n + 1):  # Итерируемся от 1 до 500
            customers.append({
                'customer_id': i,  # Суррогатный первичный ключ покупателя
                'full_name': fake.name(),  # Генерация полноценного русского ФИО
                'email': fake.unique.email(),  # Уникальный email, защищенный от дублирования модификатором unique
                'phone': fake.phone_number(),  # Строковый формат номера телефона
                'registration_date': fake.date_between(start_date='-2y', end_date='-1m')  # Дата регистрации за последние 2 года
            })
        return pd.DataFrame(customers)  # Конвертируем список словарей в объект DataFrame

    # Внутренний генератор 1000 товаров со строгим распределением по категориям и осмысленными именами
    def generate_products(n):
        # Массив целевых категорий интернет-магазина согласно техническому заданию
        categories = ['Электроника', 'Одежда', 'Книги', 'Дом и сад', 'Красота']
        
        # Словари с реальными названиями моделей для каждой категории товара
        real_names = {
            'Электроника': [
                'Смартфон Apple iPhone 15 Pro', 'Беспроводные наушники JBL Tune', 
                'Умные часы Xiaomi Redmi Watch', 'Ноутбук ASUS Vivobook 15', 
                'Планшет Samsung Galaxy Tab', 'Портативная колонка Яндекс Станция', 
                'Монитор LG UltraGear 27"', 'Игровая приставка Sony PlayStation 5', 
                'Внешний аккумулятор Xiaomi Power Bank', 'Робот-пылесос Roborock Q7'
            ],
            'Одежда': [
                'Футболка хлопковая OverSize', 'Джинсы классические Straight', 
                'Худи однотонное с капюшоном', 'Куртка демисезонная ветрозащитная', 
                'Спортивные штаны Nike', 'Кроссовки беговые повседневные', 
                'Рубашка в клетку фланелевая', 'Носки спортивные (комплект 5 пар)', 
                'Кепка однотонная базовая', 'Свитшот трикотажный'
            ],
            'Книги': [
                'Изучаем Python (Марк Лутц)', 'Грокаем алгоритмы (Адитья Бхаргава)', 
                'Чистый код (Роберт Мартин)', '1984 (Джордж Оруэлл)', 
                'Совершенный код (Стив Макконнелл)', 'Психология влияния (Роберт Чалдини)',
                'Думай медленно... решай быстро (Даниэль Канеман)', 'Атлант расправил плечи',
                'Богатый папа, бедный папа', 'Паттерны проектирования (GoF)'
            ],
            'Дом и сад': [
                'Набор кухонных ножей из стали', 'Светодиодная настольная лампа', 
                'Ортопедическая подушка', 'Термокружка из нержавейки', 
                'Коврик для йоги нескользящий', 'Садовый секатор профессиональный',
                'Набор горшков для цветов', 'Плед флисовый уютный',
                'Увлажнитель воздуха ультразвуковой', 'Шторы блэкаут на люверсах'
            ],
            'Красота': [
                'Крем для лица увлажняющий', 'Шампунь бессульфатный восстанавливающий', 
                'Парфюмерная вода мужская 50мл', 'Сыворотка с гиалуроновой кислотой', 
                'Маска для волос питательная', 'Электрическая зубная щетка',
                'Тоник для лица очищающий', 'Масло для бороды и усов',
                'Патчи для глаз гидрогелевые', 'Соль для ванны с ароматом лаванды'
            ]
        }
        
        products = []  # Список для сбора записей о продуктах
        for i in range(1, n + 1):  # Цикл до 1000 товаров
            category = random.choice(categories)  # Случайно выбираем категорию из пяти доступных
            # Берем случайное базовое имя товара из соответствующего списка словаря real_names
            base_name = random.choice(real_names[category])
            # Формируем уникальное имя, добавляя к модели случайный серийный индекс/модификатор
            full_name = f"{base_name} (Модель {random.randint(10, 99)})"
            
            products.append({
                'product_id': i,  # Уникальный ID товара
                'name': full_name,  # Красивое человеческое название вместо бреда из слов Faker
                'category': category,  # Текстовое наименование категории
                'price': random.randint(300, 50000),  # Случайная цена в диапазоне от 300 до 50 000 руб.
                'stock': random.randint(0, 150)  # Остаток единиц товара на складе компании
            })
        return pd.DataFrame(products)  # Превращаем накопленные записи в датафрейм

    # Внутренний генератор 3000 заказов интернет-магазина
    def generate_orders(n, customer_ids):
        orders = []  # Инициализация списка заказов
        # Прописываем статусы, где Completed дублируется для повышения вероятности успешной покупки
        statuses = ['Completed', 'Completed', 'Completed', 'Processing', 'Cancelled'] 
        for i in range(1, n + 1):
            orders.append({
                'order_id': i,  # ID заказа
                'customer_id': random.choice(customer_ids),  # Внешний ключ: привязка к случайному ID покупателя
                'order_date': fake.date_between(start_date='-1y', end_date='today'),  # Дата чека за последний год
                'status': random.choice(statuses),  # Статус транзакции
                'total_amount': 0  # Технический ноль. Итоговая сумма будет пересчитана ниже по чеку
            })
        return pd.DataFrame(orders)

    # Внутренний генератор наполнения заказов (конкретные товарные позиции в чеке)
    def generate_order_items(orders_df, products_df):
        items = []  # Список позиций в чеках
        item_id = 1  # Счетчик строк товарных позиций
        # Оптимизация: переводим товары в быстрый словарь {product_id: price} для работы в ОЗУ
        product_dict = products_df.set_index('product_id')['price'].to_dict()
        
        for _, order in orders_df.iterrows():  # Итерируемся по каждой строке сгенерированных заказов
            n_items = random.randint(1, 3)  # Рандомно определяем, что в одном чеке будет от 1 до 3 товаров
            chosen_products = random.sample(list(product_dict.keys()), n_items)  # Выбираем неповторяющиеся ID товаров
            
            for p_id in chosen_products:  # Добавляем каждую позицию в чек
                qty = random.randint(1, 2)  # Покупатель приобретает либо 1, либо 2 единицы этой позиции
                price = product_dict[p_id]  # Вытаскиваем цену из словаря
                items.append({
                    'item_id': item_id,  # Уникальный ID записи позиции чека
                    'order_id': order['order_id'],  # Внешний ключ: связь с родительским заказом
                    'product_id': p_id,  # Внешний ключ: связь с покупаемым продуктом
                    'quantity': qty,  # Количество купленного товара
                    'price_at_order': price  # Фиксация исторической цены на момент покупки (для BI-анализа)
                })
                item_id += 1  # Инкрементируем счетчик ID позиций
        return pd.DataFrame(items)

    # Внутренний генератор 1200 отзывов на товары
    def generate_reviews(n, customer_ids, product_ids):
        reviews = []  # Список отзывов
        for i in range(1, n + 1):
            reviews.append({
                'review_id': i,  # Первичный ключ отзыва
                'product_id': random.choice(product_ids),  # Связь с продуктом, на который пишут отзыв
                'customer_id': random.choice(customer_ids),  # Связь с покупателем, который оставил отзыв
                'rating': random.randint(1, 5),  # Выставляемая оценка от 1 до 5 звезд
                'review_text': fake.sentence()  # Случайное текстовое предложение в качестве комментария
            })
        return pd.DataFrame(reviews)

    # Запускаем конвейер последовательного вызова функций генерации
    cust_df = generate_customers(500)
    prod_df = generate_products(1000)
    ord_df = generate_orders(3000, cust_df['customer_id'].tolist())
    items_df = generate_order_items(ord_df, prod_df)
    rev_df = generate_reviews(1200, cust_df['customer_id'].tolist(), prod_df['product_id'].tolist())

    # Агрегация данных Pandas: рассчитываем финальную стоимость каждого заказа
    items_df['total_item_price'] = items_df['quantity'] * items_df['price_at_order']
    order_sums = items_df.groupby('order_id')['total_item_price'].sum().to_dict()
    ord_df['total_amount'] = ord_df['order_id'].map(order_sums)  # Маппим рассчитанные суммы на таблицу заказов

    # Определяем путь к папке обмена данными внутри контейнеров Airflow
    data_dir = '/opt/airflow/data'
    os.makedirs(data_dir, exist_ok=True)  # Создаем директорию, если она физически отсутствует
    
    # Конвертируем готовые датафреймы в плоские CSV-файлы
    cust_df.to_csv(f'{data_dir}/customers.csv', index=False)
    prod_df.to_csv(f'{data_dir}/products.csv', index=False)
    ord_df.to_csv(f'{data_dir}/orders.csv', index=False)
    items_df.to_csv(f'{data_dir}/order_items.csv', index=False)
    rev_df.to_csv(f'{data_dir}/reviews.csv', index=False)
    
    # Сигнализируем в консоль об успешном завершении сессии генерации сырых файлов
    print("Все 5 CSV-файлов успешно записаны в директорию /data.")

if __name__ == "__main__":
    generate_synthetic_data()  # Запуск генератора при прямом выполнении скрипта через консоль
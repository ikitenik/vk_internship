import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import csv


# Функция, возвращающая день недели перед вводимой датой
def calculate_date(date, days_to_decrease):
    return (date - timedelta(days=days_to_decrease + 1)).strftime("%Y-%m-%d")


# Функция, возвращающая данные из уже подсчитанных файлов calculated_days
def take_calculated_data(file):
    calculated_data = pd.read_csv(file, sep=';',
                                  usecols=["email", "create_count",
                                           "read_count", "update_count", "delete_count"])
    calculated_data = calculated_data.to_dict(orient="records")
    return calculated_data


# Функция, считающая количество событий за один день
def calculate_actions(data):
    temp_storage = {}
    for user in data:
        temp_storage.setdefault(user["email"], {"create_count": 0, "read_count": 0,
                                                "update_count": 0, "delete_count": 0})

        if user["action"] == "CREATE":
            temp_storage[user["email"]]["create_count"] += 1

        if user["action"] == "READ":
            temp_storage[user["email"]]["read_count"] += 1

        if user["action"] == "UPDATE":
            temp_storage[user["email"]]["update_count"] += 1

        if user["action"] == "DELETE":
            temp_storage[user["email"]]["delete_count"] += 1


    data = [{"email": email, **data} for email, data in temp_storage.items()]
    return data


# Функция, добавляющая подсчитанные события за день в словарь
def add_data(storage, data_to_add):
    for user in data_to_add:
        storage.setdefault(user["email"], {"create_count": 0, "read_count": 0,
                                                "update_count": 0, "delete_count": 0})
        storage[user["email"]]["create_count"] += user["create_count"]
        storage[user["email"]]["read_count"] += user["read_count"]
        storage[user["email"]]["update_count"] += user["update_count"]
        storage[user["email"]]["delete_count"] += user["delete_count"]


def make_csv_file(data, file):
    with open(file, 'w+', newline='') as out:
        fieldnames = ["email", "create_count",
                                           "read_count", "update_count", "delete_count"]
        writer = csv.DictWriter(out, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    date = datetime.strptime(sys.argv[1], "%Y-%m-%d")

    # Получаем даты дней для подсчета, совпадающие с существующими файлами логов
    dates = [calculate_date(date, i) for i in range(7)
             if os.path.exists(f"input/{calculate_date(date, i)}.csv")]

    # Словарь вида {"email": {create_count: 0,...}}, хранящий е-мейлы и количество событий пользователей
    counts = {}
    for date in dates:

        calculated_file = f"calculated_days/{date}.csv"
        # Если файл уже подсчитан
        if os.path.exists(calculated_file):
            add_data(counts, take_calculated_data(calculated_file))
            continue

        current_file = f"input/{date}.csv"
        if os.path.exists(current_file):
            data_from_current_file = pd.read_csv(current_file, sep=';',
                                                 usecols=["email", "action"])
            data_from_current_file = data_from_current_file.to_dict(orient="records")

            calculated_data = calculate_actions(data_from_current_file) 

            # Добавляем промежуточные вычисления в файл
            make_csv_file(calculated_data, f"calculated_days/{date}.csv")
            # Добавляем данные в общий список
            add_data(counts, calculated_data)

    # Преобразуем словарь в список словарей
    data = [{"email": email, **data} for email, data in counts.items()]
    make_csv_file(data, f"output/{sys.argv[1]}.csv")
import pandas as pd

data = pd.read_csv("G:/InOnAt/Python_task2/PandasNotNumpy/data/adult.data.csv")
data.head()

# 1___________________________________________________________________________________
print("Задача 1: Посчитайте, сколько мужчин и женщин (признак sex) представлено в этом датасете")
sex_counts = data['sex'].value_counts()
print(sex_counts, '\n')

# 2___________________________________________________________________________________
print("Задача 2: Каков средний возраст мужчин (признак age) по всему датасету?")
avg_age_male = data[data['sex'] == 'Male']['age'].mean()        # .mean() используется для вычи сред ариф значений
print(avg_age_male, '\n')

# 3___________________________________________________________________________________
print("Задача 3: Какова доля граждан Соединенных Штатов (признак native-country)?")
usa_count = data[data['native-country'] == 'United-States']['native-country'].count()
all_count = data['native-country'].count()
print(usa_count / all_count * 100, '\n')

# 4-5___________________________________________________________________________________
print("Задача 4-5: Рассчитайте среднее значение и среднеквадратичное отклонение возраста тех,"
      " кто получает более 50K в год (признак salary) и тех, кто получает менее 50K в год")
age_bigger_then_50K = data[data['salary'] == '>50K']['age']
print('Bigger than 50K:')
print(f'Mean is: {age_bigger_then_50K.mean()}')
print(f'Std is: {age_bigger_then_50K.std()}')       # .std() - стандартное отклонение или разброс данных считаем

age_less_then_50K = data[data['salary'] == '<=50K']['age']
print('Less than 50K:')
print(f'Mean is: {age_less_then_50K.mean()}')
print(f'Std is: {age_less_then_50K.std()}', '\n')

# 6___________________________________________________________________________________
print("Задача 6: Правда ли, что люди, которые получают больше 50k, имеют минимум высшее образование? "
      "(признак education – Bachelors, Prof-school, Assoc-acdm, Assoc-voc, Masters или Doctorate)")
higher_education = ["Bachelors", "Prof-school", "Assoc-acdm", "Assoc-voc", "Masters", "Doctorate"]
is_more50k_educated = data[-data['education'].isin(higher_education)].size == 0

print(f'Правда ли, что люди, которые получают больше 50k, имеют минимум высшее образование? {is_more50k_educated}', '\n')

# 7___________________________________________________________________________________
print("Задача 7:  Выведите статистику возраста для каждой расы (признак race) и каждого пола. "
      "Используйте groupby и describe. Найдите таким образом максимальный возраст мужчин расы Asian-Pac-Islander.")
male_data = data[data['sex'] == 'Male']
A = male_data.groupby('race')['age'].describe()
max_age_male = A['max']['Asian-Pac-Islander']
print(max_age_male, '\n')


# 8___________________________________________________________________________________
print("Задача 8:   Среди кого больше доля зарабатывающих много (>50K): среди женатых или холостых мужчин "
      "(признак marital-status)? Женатыми считаем тех, у кого marital-status начинается с Married (Married-civ-spouse,"
      "Married-spouse-absent или Married-AF-spouse), остальных считаем холостыми.")
rich_men = data[(data['sex'] == 'Male') & (data['salary'] == '>50K')]
married_rich_men = rich_men[rich_men['marital-status'].str.startswith("Married")]
not_married_rich_men = rich_men[-rich_men['marital-status'].str.startswith("Married")]

if married_rich_men.size > not_married_rich_men.size:
    print("Среди женатых больше")
elif married_rich_men.size < not_married_rich_men.size:
    print('Среди холостых больше')
else:
    print('Одинаковое кол-во холостых и женатых богачей')
# 9___________________________________________________________________________________
print("Задача 9:  Какое максимальное число часов человек работает в неделю (признак hours-per-week)? "
      "Сколько людей работают такое количество часов и каков среди них процент зарабатывающих много?")
max_hours = data['hours-per-week'].max()
max_hours_workers = data[data['hours-per-week'] == max_hours]
max_hours_more50k_percent = (max_hours_workers['salary'] == '>50K').mean()

print(max_hours)
print(max_hours_workers.size)
print(max_hours_more50k_percent, '\n')

# 10___________________________________________________________________________________
print("Задача 10: Посчитайте среднее время работы (hours-per-week) зарабатывающих мало"
      " и много (salary) для каждой страны (native-country).")
hours_stats = data.groupby(['native-country', 'salary'])['hours-per-week'].mean().unstack()
print(hours_stats)
print()

# 11___________________________________________________________________________________
print("Задача 11: Сгруппируйте людей по возрастным группам young, adult, retiree, "
      "где: young соответствует 16-35 лет, adult - 35-70 лет, retiree - 70-100 лет. "
      "Проставьте название соответсвтуещей группы для каждого человека в новой колонке AgeGroup")

bins = [16, 35, 70, 100]
labels = ["young", "adult", "retiree"]
data["AgeGroup"] = pd.cut(data["age"], bins=bins, labels=labels, right=False)

print(data[["age", "AgeGroup"]].head(20))
print()

# 12-13___________________________________________________________________________________
print("Задача 12-13: Определите количество зарабатывающих >50K в каждой из возрастных групп (колонка AgeGroup),"
      " а также выведите название возрастной группы, в которой чаще зарабатывают больше 50К (>50K)")
age_more50k_group = data[data['salary'] == '>50K']['AgeGroup'].value_counts()
print(age_more50k_group)
print(f'Возрастная группа, где больше всего зарабатывают много: {age_more50k_group.idxmax()}', '\n')

# 14___________________________________________________________________________________
print("Задача 14:  Сгруппируйте людей по типу занятости (колонка occupa) и определите количество людей в кажд группе."
      " После чего напишите функциюю фильтрации filter_func, которая будет возвращать только те группы,"
      " в которых средний возраст (колонка age) не больше 40"
      " и в которых все работники отрабатывают более 5 часов в неделю (колонка hours-per-week)")
occupation_stats = data['occupation'].value_counts()
print(occupation_stats)


def filter_func(group):
    return (group["age"].mean() <= 40) and (group["hours-per-week"].min() > 5)

filtered_groups = data.groupby("occupation").filter(filter_func)['occupation'].value_counts()
print("Отфильтрованные группы:\n", filtered_groups)




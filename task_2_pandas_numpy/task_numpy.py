import numpy as np
import time, math
from collections import Counter
from scipy.spatial.distance import cdist

# 0_1____________________________________________________________________________________
print("0_1_Дан случайный массив, поменять знак у элементов, значения которых между 3 и 8")
array_1 = np.random.randint(0, 10, 10)
print(array_1)


def change_sign(array_1):
    array_1[(3 < array_1) & (array_1 < 8)] *= -1                # сравниваем булевые массивы (&)
    return array_1


print(change_sign(array_1), '\n')

# 0_2___________________________________________________________________________________
print("0_2_Заменить максимальный элемент случайного массива на 0")
array_2 = np.random.randint(0, 20, 10)
print(array_2)


def change_max(array_2):
    array_2[array_2 == max(array_2)] = 0
    return array_2


print(change_max(array_2), '\n')

# 0_3___________________________________________________________________________________
print("0_3_Построить прямое произведение массивов (все комбинации с каждым элементом). На вход подается двумерный массив")
a = np.random.randint(0, 10, size=(3, 3))


def decard_mull(array_3):
    mesh = np.meshgrid(*array_3, indexing='ij')                   # создаём координатную сетку в виде 3D куба
    print(mesh)
    # *array_3 - расспаков строкии в аргументы
    product = np.stack(mesh, axis=-1).reshape(-1, len(array_3))   # .stack() - на основе коор массивов создаст массив,
    # где каждая ячейка будет содержать кортеж из 3 элем
    # .reshape() - развернёт куб в нужны массив нужного размера

    return product


print(a, decard_mull(a), sep='\n\n', end='\n\n')

# 0_4___________________________________________________________________________________
print("0_4_Даны 2 массива A (8x3) и B (2x2). Найти строки в A, которые содержат элементы из каждой строки в B, "
      "независимо от порядка элементов в B")

a = np.random.randint(0, 5, size=(8, 3))
b = np.random.randint(0, 5, size=(2, 2))

print("Массив A:")
print(a)
print("Массив B:")
print(b)

b_set = set(b.flatten())                             # .flatten() - преобразовали элементы в одномер массив
# set - удалил дубликаты

result = a[np.any(np.isin(a, list(b_set)), axis=1)]  # .insin() - провер содержаться ли элементы А в В
# вернёт размер массива А, но содерж true
# .any() - проверяет по строкам массив, ищет хотя бы одно true. вернёт одном массив удолет услов
# a[] - булевая индексация, вернёт те строки А, для которых соотв элем массива равен true
print("Результат - строки A, содерж хотя бы один элемент из B:")
print(result, '\n')

# 0_5___________________________________________________________________________________
print("0_5_Дана 10x3 матрица, найти строки из неравных значений "
      "(например строка [2,2,3] остается, строка [3,3,3] удаляется)")
array_5 = np.random.randint(0, 5, size=(10, 3))
print(array_5)
print(array_5.shape)
result = array_5[np.apply_along_axis(lambda x: len(np.unique(x)) != 1, axis=1, arr=array_5)]
print(result)
print(result.shape, '\n')
# np.unique(x) - вернёт массив из уникальных хначений или одно значение, если уникальных не найдёться
# .apply_along_axis() - применяется к одномерным срезам в доль указанной оси
# axis=1 - разрез по строкам, 0 по столбцам
# будет возвращён булевый массив, по которому будут вывеведены удолетворяющие строки

# 0_6___________________________________________________________________________________
print("0_6_Дан двумерный массив. Удалить те строки, которые повторяются")

array_6 = np.random.randint(0, 2, size=(3, 3))
print(array_6, '\n')

result = np.unique(array_6, axis=0)         # axis=0 - сравнить по строкам
print(result, '\n')

# 1___________________________________________________________________________________
print("Задача 1: Подсчитать произведение ненулевых элементов на диагонали прямоугольной матрицы."
      " Например, для X = np.array([[1, 0, 1], [2, 0, 2], [3, 0, 3], [4, 4, 4]]) ответ 3.")


arr = np.array([[1, 0, 1], [2, 0, 2], [3, 0, 3], [4, 4, 4]])
X = arr.tolist()
print(arr)

# Python__
answer = 1
for i in range(len(X[0])):
    if X[i][i]:
        answer *= X[i][i]

print("python version:", answer)

# Numpy__
result = np.diagonal(arr)
result = result[result != 0]
print("numpy version:", result.prod(), '\n')

# 2___________________________________________________________________________________
print("Задача 2: Даны два вектора x и y. Проверить, задают ли они одно и то же мультимножество."
      "Например, для x = np.array([1, 2, 2, 4]), y = np.array([4, 2, 1, 2]) ответ True.")
x_n = np.array([1, 2, 2, 4])
y_n = np.array([4, 2, 1, 2])
x = x_n.tolist()
y = y_n.tolist()
print(x)
print(y)

# Python
x.sort()
y.sort()
print("python version:", x == y)

# Numpy
print("numpy version:", np.array_equal(np.sort(x_n), np.sort(y_n)), '\n')

# 3___________________________________________________________________________________
print("Задача 3: Найти максимальный элемент в векторе x среди элементов, "
      "перед которыми стоит ноль. Например, для x = np.array([6, 2, 0, 3, 0, 0, 5, 7, 0]) ответ 5.")
xn3 = np.array([6, 2, 0, 3, 0, 0, 5, 7, 0])

# Python
x = xn3.tolist()
max = None
for i in range(1, len(x)):                          # начинаем с 1, чтобы можно было бы пров пред элем не выходя за гран
    if x[i - 1] == 0 and (not max or x[i] > max):
        max = x[i]
print("python version:", max)

# Numpy
zero = xn3 == 0
print("numpy version:", xn3[1:][zero[:-1]].max(), '\n')

# 4___________________________________________________________________________________
print("Задача 4: Реализовать кодирование длин серий (Run-length encoding)."
      " Для некоторого вектора x необходимо вернуть кортеж из двух векторов одинаковой длины."
      " Первый содержит числа, а второй - сколько раз их нужно повторить."
      "Например, для x = np.array([2, 2, 2, 3, 3, 3, 5]) ответ (np.array([2, 3, 5]), np.array([3, 3, 1])).")
# Numpy
xn4 = np.array([2, 2, 2, 3, 3, 3, 5])
unique, counts = np.unique(xn4, return_counts=True)       # unique() - находит все уник числа в массиве
# и сортирует их по возрастанию
# return_counts=True - счтч сколько раз каждое уник знач встреч
print("numpy version:", unique, counts)

# Python
x4 = Counter(xn4.tolist())      # Counter() - подсчёт сколько раз каждый элемент встречается в списке {num: count}
nums = [i for i in x4.keys()]
repeat = [i for i in x4.values()]
print("python version:", nums, repeat, '\n')

# 5___________________________________________________________________________________
print("Задача 5: Даны две выборки объектов - X и Y. Вычислить матрицу евклидовых расстояний между объектами. "
      "Сравните с функцией scipy.spatial.distance.cdist по скорости работы.")

X = np.random.rand(5, 5)
Y = np.random.rand(5, 5)

X_python = X.tolist()
Y_python = Y.tolist()


#Python
start0 = time.time()
d0 = [[math.sqrt(sum((xi - yi) ** 2 for xi, yi in zip(x, y))) for y in Y_python] for x in X_python]
end0 = time.time()

#NumPy
start1 = time.time()
X_sq = np.sum(X**2, axis=1)[:, np.newaxis]  # изменям форму массивов на столбцы и строки соответ
Y_sq = np.sum(Y**2, axis=1)[np.newaxis, :]
dists_sq = X_sq + Y_sq - 2 * X @ Y.T        # матричное умножение на транспонир матр X @ Y.T
d1 = np.sqrt(dists_sq)
end1 = time.time()

# scipy
start2 = time.time()
d2 = cdist(X, Y, metric='euclidean')        # спец функ для вычисления попарных растояний
end2 = time.time()

print(f"pytohn version: {d0}\n")
print(f"numpy version: {d1}\n")
print(f"SciPy version: {d2}\n")
print(f"python time: {end0 - start0}")
print(f"numpy time: {end1 - start1}")
print(f"SciPy time: {end2 - start2}")
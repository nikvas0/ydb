# DROP GROUP

Удаляет указанную группу. Для одного оператора вы можете указать несколько групп.

Синтаксис:

```yql
DROP GROUP [ IF EXISTS ] group_name [, ...]
```

* `IF EXISTS` — не выводить ошибку, если группа не существует.
* `group_name` — имя группы, которого нужно удалить.

## Встроенные группы

{% include [!](../_includes/initial_groups_and_users.md) %}

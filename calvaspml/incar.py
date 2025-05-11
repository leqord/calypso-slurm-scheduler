import re
from pathlib import Path

class IncarFile:
    """
    Редактирование INCAR «на месте»:
    - get(): найти первый тэг и вернуть его значение (bool/int/float/str)
    - set(): заменить все вхождения тэга или добавить его в конец
    - delete(): удалить все строки с этим тэгом
    Комментарии и прочие строки сохраняются без изменений.
    """
    BOOL_MAP = {'.TRUE.': True, '.FALSE.': False}
    INV_BOOL_MAP = {True: '.TRUE.', False: '.FALSE.'}

    def __init__(self, filepath: Path):
        self.filepath = filepath
        if not self.filepath.exists():
            raise FileNotFoundError(f"{self.filepath} not found")

    def _parse_value(self, raw: str):
        v = raw.strip()
        up = v.upper()
        if up in self.BOOL_MAP:
            return self.BOOL_MAP[up]
        if re.fullmatch(r"[+-]?\d+", v):
            return int(v)
        if re.fullmatch(r"[+-]?(\d+\.\d*|\.\d+)([eE][+-]?\d+)?", v) \
           or re.fullmatch(r"[+-]?\d+[eE][+-]?\d+", v):
            return float(v)
        return v  # всё остальное — строка

    def get(self, tag, default=None):
        """Сканируем файл — возвращаем первое значение тэга или default."""
        pattern = re.compile(rf"^\s*({re.escape(tag)})\s*=\s*([^#!]+)")
        with self.filepath.open() as f:
            for line in f:
                m = pattern.match(line)
                if m:
                    raw_val = m.group(2)
                    return self._parse_value(raw_val)
        return default

    def set(self, tag, value):
        """
        Заменяем все вхождения tag или добавляем в конец.
        Сохраняем комментарии.
        """
        val_str = (self.INV_BOOL_MAP[value]
                   if isinstance(value, bool)
                   else str(value))
        tag_pat = re.compile(rf"^(\s*)({re.escape(tag)})\s*=\s*([^#!]*)([#!].*)?$")
        lines = []
        found = False

        for line in self.filepath.open():
            m = tag_pat.match(line)
            if m:
                indent, key, old_val, comment = m.groups()
                comment = comment or ""
                new_line = f"{indent}{key} = {val_str}{comment}\n"
                lines.append(new_line)
                found = True
            else:
                lines.append(line)

        if not found:
            # добавим в конец (с пустой строкой перед, если нужно)
            if lines and not lines[-1].endswith("\n"):
                lines[-1] += "\n"
            lines.append(f"{tag} = {val_str}\n")

        self.filepath.write_text("".join(lines))

    def delete(self, tag):
        """Удаляем все строки, где стоит tag = ..."""
        tag_pat = re.compile(rf"^\s*{re.escape(tag)}\s*=")
        new_lines = []
        for line in self.filepath.open():
            if not tag_pat.match(line):
                new_lines.append(line)
        self.filepath.write_text("".join(new_lines))

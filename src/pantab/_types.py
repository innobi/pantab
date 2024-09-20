from typing import Optional, Protocol, Union, runtime_checkable


@runtime_checkable
class TableauName(Protocol):
    @property
    def unescaped(self) -> str:
        ...


@runtime_checkable
class TableauTableName(Protocol):
    @property
    def name(self) -> TableauName:
        ...

    @property
    def schema_name(self) -> Optional[TableauName]:
        ...


TableNameType = Union[str, TableauName, TableauTableName]

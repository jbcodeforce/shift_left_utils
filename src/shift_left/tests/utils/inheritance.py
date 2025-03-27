from typing import Generic, TypeVar, Optional, List

from pydantic import BaseModel

TypeX = TypeVar('TypeX')
TypeY = TypeVar('TypeY')
TypeZ = TypeVar('TypeZ')


class BaseClass(BaseModel, Generic[TypeX, TypeY]):
    x: TypeX
    y: TypeY


class ChildClass(BaseClass[int, TypeY], Generic[TypeY, TypeZ]):
    z: TypeZ

# Parametrize `TypeY` with `str`:
print(ChildClass[str, int](x='1', y='y', z='3'))

class TreeNode(BaseModel):
    name: str
    children: Optional[List['TreeNode']] = None
    parents: Optional[List['TreeNode']] = None

tree = TreeNode(
    name="root",
    children=[
        TreeNode(name="child1", parents=[ TreeNode(name='root')]),
        TreeNode(name="child2", parents=[TreeNode(name='root')], children=[TreeNode(name="grandchild1")]),
    ],
    parents=[
        TreeNode(name="parent1"),
        TreeNode(name="parent2"),
    ]
)

print(tree)

class InnerModel(BaseModel):
    value: int

class OuterModel(BaseModel):
    inner: InnerModel

# Example Usage
original_data = OuterModel(inner=InnerModel(value=1))

# Shallow copy
shallow_copied_data = original_data.model_copy()
shallow_copied_data.inner.value = 2
print(f"Original (shallow copy): {original_data.inner.value}")
print(f"Shallow Copy: {shallow_copied_data.inner.value}")

# Deep copy
deep_copied_data = original_data.model_copy(deep=True)
deep_copied_data.inner.value = 3
print(f"Original (deep copy): {original_data.inner.value}")
print(f"Deep Copy: {deep_copied_data.inner.value}")


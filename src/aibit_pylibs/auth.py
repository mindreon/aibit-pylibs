# auth.py

from fastapi import Depends, HTTPException, status, Request, Query, Cookie
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel
from datetime import datetime, timedelta, timezone
from typing import Optional, Union

# --- 配置 ---
# 这个密钥应该非常复杂，并且保存在环境变量中，而不是硬编码在代码里
SECRET_KEY = "your-super-secret-key-that-is-long-and-random"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24

# OAuth2PasswordBearer 会从请求的 Authorization Header 中寻找 Bearer Token
# tokenUrl 是一个相对路径，指向我们之后会创建的、用于获取 token 的接口
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

# --- Pydantic 模型 ---
# 用于在解码 Token 后进行数据校验
class TokenData(BaseModel):
    """
    JWT Token中存储的用户数据模型
    
    标准JWT字段：
    - sub (subject): 用户标识符，通常是用户ID
    - exp (expiration): 过期时间
    - iat (issued at): 签发时间
    
    自定义字段：
    - username: 用户名
    - email: 用户邮箱
    - role: 用户角色
    - permissions: 用户权限列表
    """
    # 标准JWT字段
    sub: str  # 用户ID，这是JWT标准字段
    exp: Optional[datetime] = None  # 过期时间
    iat: Optional[datetime] = None  # 签发时间
    
    # 自定义用户信息字段
    username: str
    user_id: str
    group_id: str
    group_name: str
    tenant_name:str
    tenant_id: str
    role: Optional[str] = None
    permissions: Optional[list[str]] = None
    is_active: Optional[bool] = None

# --- Token 创建函数 ---
def create_access_token(
    user_data: dict, 
    expires_delta: timedelta | None = None,
    include_standard_fields: bool = True
) -> str:
    """
    创建一个新的 Access Token
    
    参数：
    - user_data: 包含用户信息的字典
    - expires_delta: 可选的过期时间增量
    - include_standard_fields: 是否包含标准JWT字段（exp, iat）
    
    返回：
    - 编码后的JWT字符串
    """
    # 复制用户数据，避免修改原始数据
    to_encode = user_data.copy()
    
    # 确保sub字段存在（用户ID）
    if "sub" not in to_encode:
        raise ValueError("user_data must contain 'sub' field (user ID)")
    
    # 添加标准JWT字段
    if include_standard_fields:
        # 设置过期时间
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
        
        to_encode.update({
            "exp": expire,
            "iat": datetime.now(timezone.utc)  # 签发时间
        })
    
    # 使用 PyJWT 编码
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# --- 新的Token获取函数 ---
async def get_token_from_multiple_sources(
    request: Request,
    bearer_token: Optional[str] = Depends(oauth2_scheme),
    url_token: Optional[str] = Query(None, alias="token"),
    cookie_token: Optional[str] = Cookie(None, alias="token")
) -> Optional[str]:
    """
    从多个地方获取token，按优先级顺序：
    1. Bearer Token (Authorization Header)
    2. URL参数中的token
    3. Cookies中的token
    
    参数说明：
    - request: FastAPI请求对象，用于手动检查
    - bearer_token: 从Authorization Header获取的Bearer token
    - url_token: 从URL查询参数获取的token
    - cookie_token: 从Cookies获取的token
    
    返回：
    - 找到的第一个有效token，如果没有找到则返回None
    """
    # 优先级1: Bearer Token (Authorization Header)
    if bearer_token:
        return bearer_token
    
    # 优先级2: URL参数中的token
    if url_token:
        return url_token
    
    # 优先级3: Cookies中的token
    if cookie_token:
        return cookie_token
    
    # 如果都没有找到，返回None
    return None

# --- 修改后的依赖项：获取当前用户 ---
async def get_jwt_user(
    token: Optional[str] = Depends(get_token_from_multiple_sources)
) -> TokenData:
    """
    增强版的用户认证依赖项，支持从多个地方获取token
    
    返回：
    - TokenData对象，包含JWT中的所有用户信息
    
    异常：
    - HTTPException 401: 如果没有找到token或token无效
    """
    # 如果没有找到任何token，抛出认证异常
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No valid token found. Please provide token via Bearer, URL parameter, or cookie",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # 定义凭证无效时的异常
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        # 解码JWT
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        
        # 验证必需的字段
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
        
        # 创建TokenData对象，包含所有JWT字段
        token_data = TokenData(**payload)
        
    except JWTError:
        # 如果解码失败（比如签名不对、过期等），jose 库会抛出 JWTError
        raise credentials_exception
    except Exception as e:
        # 处理其他可能的错误（比如数据验证失败）
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token validation failed: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )
 
    return token_data

# --- 便捷函数：创建包含完整用户信息的token ---
def create_user_token(
    user_id: str,
    username: str,
    group_id: str,
    group_name: str,
    tenant_name: str,
    tenant_id: str,
    role: Optional[str] = None,
    permissions: Optional[list[str]] = None,
    is_active: bool = True,
    expires_delta: Optional[timedelta] = None
) -> str:
    """
    创建包含完整用户信息的JWT token
    
    参数：
    - user_id: 用户唯一标识符（必需）
    - username: 用户名（必需）
    - group_id: 用户组ID（必需）
    - group_name: 用户组名称（必需）
    - tenant_name: 租户名称（必需）
    - tenant_id: 租户ID（必需）
    - role: 用户角色
    - permissions: 用户权限列表
    - is_active: 用户是否激活
    - expires_delta: 可选的过期时间增量
    
    返回：
    - 编码后的JWT字符串
    
    示例：
    ```python
    token = create_user_token(
        user_id="12345",
        username="john_doe",
        group_id="12345",
        group_name="Group 1",
        tenant_name="Tenant 1",
        tenant_id="12345",
        role="user",
        permissions=["read", "write"],
        is_active=True
    )
    ```
    """
    user_data = {
        "sub": user_id,  # JWT标准字段：用户ID
        "username": username,
        "group_id": group_id,
        "group_name": group_name,
        "tenant_name": tenant_name,
        "tenant_id": tenant_id,
        "role": role,
        "permissions": permissions,
        "is_active": is_active
    }
    
    # 移除None值，避免在JWT中存储空值
    user_data = {k: v for k, v in user_data.items() if v is not None}
    
    return create_access_token(user_data, expires_delta)
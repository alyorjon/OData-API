from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
import re

app = FastAPI(
    title="OData API",
    description="RESTful OData API using FastAPI",
    version="1.0.0"
)

# === Models ===
class CustomerStatus(str, Enum):
    ACTIVE = "Active"
    INACTIVE = "Inactive"
    SUSPENDED = "Suspended"

class Customer(BaseModel):
    CustomerID: int
    CustomerName: str
    Email: str
    Phone: str
    City: str
    Country: str
    Status: CustomerStatus
    CreatedDate: datetime
    CreditLimit: float

class Order(BaseModel):
    OrderID: int
    CustomerID: int
    OrderDate: datetime
    TotalAmount: float
    Status: str
    Items: List[str] = []

class ODataResponse(BaseModel):
    value: List[Dict[str, Any]]
    count: Optional[int] = None

# === Sample Data ===
customers_data = [
    Customer(
        CustomerID=1,
        CustomerName="Tech Solutions LLC",
        Email="contact@techsolutions.com",
        Phone="+1-555-0123",
        City="New York",
        Country="USA",
        Status=CustomerStatus.ACTIVE,
        CreatedDate=datetime(2023, 1, 15),
        CreditLimit=50000.0
    ),
    Customer(
        CustomerID=2,
        CustomerName="Global Trading Co",
        Email="info@globaltrading.com",
        Phone="+1-555-0456",
        City="Los Angeles",
        Country="USA",
        Status=CustomerStatus.ACTIVE,
        CreatedDate=datetime(2023, 3, 22),
        CreditLimit=75000.0
    ),
    Customer(
        CustomerID=3,
        CustomerName="European Systems",
        Email="sales@eusystems.eu",
        Phone="+49-30-123456",
        City="Berlin",
        Country="Germany",
        Status=CustomerStatus.INACTIVE,
        CreatedDate=datetime(2023, 2, 10),
        CreditLimit=30000.0
    ),
    Customer(
        CustomerID=4,
        CustomerName="Asian Enterprises",
        Email="contact@asianent.com",
        Phone="+81-3-1234567",
        City="Tokyo",
        Country="Japan",
        Status=CustomerStatus.ACTIVE,
        CreatedDate=datetime(2023, 4, 5),
        CreditLimit=60000.0
    )
]

orders_data = [
    Order(OrderID=1001, CustomerID=1, OrderDate=datetime(2024, 1, 10), TotalAmount=15000.0, Status="Completed", Items=["Laptop", "Software License"]),
    Order(OrderID=1002, CustomerID=2, OrderDate=datetime(2024, 1, 15), TotalAmount=25000.0, Status="Processing", Items=["Server", "Network Equipment"]),
    Order(OrderID=1003, CustomerID=1, OrderDate=datetime(2024, 2, 1), TotalAmount=8000.0, Status="Shipped", Items=["Tablets", "Accessories"]),
    Order(OrderID=1004, CustomerID=4, OrderDate=datetime(2024, 2, 10), TotalAmount=35000.0, Status="Completed", Items=["Enterprise Software"]),
]

# === OData Query Parser ===
class ODataQueryParser:
    @staticmethod
    def parse_filter(filter_str: str, data: List[Any]) -> List[Any]:
        """Parse $filter query parameter"""
        if not filter_str:
            return data
            
        # Simple filter parsing - in production, use proper OData parser
        filter_parts = filter_str.replace(" and ", " & ").replace(" or ", " | ")
        
        filtered_data = []
        for item in data:
            item_dict = item.dict() if hasattr(item, 'dict') else item
            
            # Simple eq (equals) filter
            if " eq " in filter_str:
                field, value = filter_str.split(" eq ")
                field = field.strip()
                value = value.strip().strip("'\"")
                
                if field in item_dict:
                    if str(item_dict[field]).lower() == value.lower():
                        filtered_data.append(item)
            
            # Simple contains filter
            elif "contains(" in filter_str:
                match = re.search(r"contains\((\w+),\s*'([^']+)'\)", filter_str)
                if match:
                    field, value = match.groups()
                    if field in item_dict and value.lower() in str(item_dict[field]).lower():
                        filtered_data.append(item)
            else:
                # If no specific filter matched, include the item
                filtered_data.append(item)
                
        return filtered_data
    
    @staticmethod
    def parse_select(select_str: str, data: List[Any]) -> List[Dict[str, Any]]:
        """Parse $select query parameter"""
        if not select_str:
            return [item.dict() if hasattr(item, 'dict') else item for item in data]
            
        fields = [field.strip() for field in select_str.split(',')]
        result = []
        
        for item in data:
            item_dict = item.dict() if hasattr(item, 'dict') else item
            selected_item = {field: item_dict.get(field) for field in fields if field in item_dict}
            result.append(selected_item)
            
        return result
    
    @staticmethod
    def parse_orderby(orderby_str: str, data: List[Any]) -> List[Any]:
        """Parse $orderby query parameter"""
        if not orderby_str:
            return data
            
        # Parse field and direction
        parts = orderby_str.split()
        field = parts[0]
        desc = len(parts) > 1 and parts[1].lower() == 'desc'
        
        try:
            return sorted(data, key=lambda x: getattr(x, field) if hasattr(x, field) else x.get(field), reverse=desc)
        except:
            return data

# === OData Endpoints ===

@app.get("/odata/$metadata", tags=["OData"])
async def get_metadata():
    """OData metadata document"""
    metadata = {
        "version": "4.0",
        "entities": {
            "Customers": {
                "properties": {
                    "CustomerID": "int",
                    "CustomerName": "string",
                    "Email": "string",
                    "Phone": "string", 
                    "City": "string",
                    "Country": "string",
                    "Status": "string",
                    "CreatedDate": "datetime",
                    "CreditLimit": "decimal"
                }
            },
            "Orders": {
                "properties": {
                    "OrderID": "int",
                    "CustomerID": "int",
                    "OrderDate": "datetime",
                    "TotalAmount": "decimal",
                    "Status": "string",
                    "Items": "array"
                }
            }
        }
    }
    return metadata

@app.get("/odata/Customers", tags=["Customers"], response_model=ODataResponse)
async def get_customers(
    filter: Optional[str] = Query(None, alias="$filter", description="Filter customers"),
    select: Optional[str] = Query(None, alias="$select", description="Select specific fields"),
    orderby: Optional[str] = Query(None, alias="$orderby", description="Order by field"),
    top: Optional[int] = Query(None, alias="$top", description="Take top N records"),
    skip: Optional[int] = Query(None, alias="$skip", description="Skip N records"),
    count: Optional[bool] = Query(False, alias="$count", description="Include count")
):
    """Get customers with OData query options"""
    
    # Apply filters
    filtered_data = ODataQueryParser.parse_filter(filter, customers_data)
    
    # Apply ordering
    ordered_data = ODataQueryParser.parse_orderby(orderby, filtered_data)
    
    # Get total count before pagination
    total_count = len(ordered_data)
    
    # Apply pagination
    if skip:
        ordered_data = ordered_data[skip:]
    if top:
        ordered_data = ordered_data[:top]
    
    # Apply field selection
    result_data = ODataQueryParser.parse_select(select, ordered_data)
    
    response = {
        "value": result_data
    }
    
    if count:
        response["count"] = total_count
        
    return response

@app.get("/odata/Customers({customer_id})", tags=["Customers"])
async def get_customer_by_id(
    customer_id: int,
    select: Optional[str] = Query(None, alias="$select"),
    expand: Optional[str] = Query(None, alias="$expand")
):
    """Get customer by ID with optional expand"""
    
    customer = next((c for c in customers_data if c.CustomerID == customer_id), None)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    result = customer.dict()
    
    # Handle $expand=Orders
    if expand and "Orders" in expand:
        customer_orders = [o.dict() for o in orders_data if o.CustomerID == customer_id]
        result["Orders"] = customer_orders
    
    # Handle $select
    if select:
        fields = [field.strip() for field in select.split(',')]
        result = {field: result.get(field) for field in fields if field in result}
    
    return result

@app.get("/odata/Orders", tags=["Orders"], response_model=ODataResponse)
async def get_orders(
    filter: Optional[str] = Query(None, alias="$filter"),
    select: Optional[str] = Query(None, alias="$select"),
    orderby: Optional[str] = Query(None, alias="$orderby"),
    top: Optional[int] = Query(None, alias="$top"),
    skip: Optional[int] = Query(None, alias="$skip"),
    count: Optional[bool] = Query(False, alias="$count")
):
    """Get orders with OData query options"""
    
    # Apply filters
    filtered_data = ODataQueryParser.parse_filter(filter, orders_data)
    
    # Apply ordering
    ordered_data = ODataQueryParser.parse_orderby(orderby, filtered_data)
    
    # Get total count
    total_count = len(ordered_data)
    
    # Apply pagination
    if skip:
        ordered_data = ordered_data[skip:]
    if top:
        ordered_data = ordered_data[:top]
    
    # Apply field selection
    result_data = ODataQueryParser.parse_select(select, ordered_data)
    
    response = {
        "value": result_data
    }
    
    if count:
        response["count"] = total_count
        
    return response

@app.get("/odata/Orders({order_id})", tags=["Orders"])
async def get_order_by_id(order_id: int):
    """Get order by ID"""
    
    order = next((o for o in orders_data if o.OrderID == order_id), None)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    return order.dict()

@app.post("/odata/Customers", tags=["Customers"])
async def create_customer(customer: Customer):
    """Create new customer"""
    
    # Check if customer ID already exists
    if any(c.CustomerID == customer.CustomerID for c in customers_data):
        raise HTTPException(status_code=400, detail="Customer ID already exists")
    
    customers_data.append(customer)
    return {"message": "Customer created successfully", "customer": customer.dict()}

@app.put("/odata/Customers({customer_id})", tags=["Customers"])
async def update_customer(customer_id: int, customer: Customer):
    """Update customer"""
    
    for i, c in enumerate(customers_data):
        if c.CustomerID == customer_id:
            customers_data[i] = customer
            return {"message": "Customer updated successfully", "customer": customer.dict()}
    
    raise HTTPException(status_code=404, detail="Customer not found")

@app.delete("/odata/Customers({customer_id})", tags=["Customers"])
async def delete_customer(customer_id: int):
    """Delete customer"""
    
    for i, c in enumerate(customers_data):
        if c.CustomerID == customer_id:
            customers_data.pop(i)
            return {"message": "Customer deleted successfully"}
    
    raise HTTPException(status_code=404, detail="Customer not found")

# === Root endpoint ===
@app.get("/", tags=["Root"])
async def root():
    """API root with available endpoints"""
    return {
        "message": "OData API with FastAPI",
        "endpoints": {
            "metadata": "/odata/$metadata",
            "customers": "/odata/Customers",
            "orders": "/odata/Orders"
        },
        "odata_query_examples": {
            "filter": "/odata/Customers?$filter=Status eq 'Active'",
            "select": "/odata/Customers?$select=CustomerName,Email",
            "orderby": "/odata/Customers?$orderby=CustomerName desc",
            "top": "/odata/Customers?$top=5",
            "expand": "/odata/Customers(1)?$expand=Orders",
            "count": "/odata/Customers?$count=true"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
from fastapi import Request


def ok(request: Request, data):
    return {"data": data, "request_id": request.state.request_id}


def page(request: Request, data, next_cursor=None, limit=20):
    return {
        "data": data,
        "page": {"limit": limit, "next_cursor": next_cursor},
        "request_id": request.state.request_id,
    }


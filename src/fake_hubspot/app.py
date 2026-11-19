"""FastAPI surface of the fake HubSpot portal (the subset of CRM v3/v4 the engine uses, plus the
single-record endpoints a naive integration would use, plus a ``/_admin`` control plane)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from fake_hubspot.hub import ApiError, FakeHub, Resp

WRITE_ACTIONS = {"upsert", "update", "archive", "create"}


def _to_response(r: Resp) -> Response:
    if r.status == 204:
        return Response(status_code=204, headers=r.headers)
    return JSONResponse(r.body, status_code=r.status, headers=r.headers)


async def _json(request: Request) -> dict[str, Any]:
    try:
        data = await request.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def create_app(hub: FakeHub | None = None) -> FastAPI:
    hub = hub or FakeHub()
    app = FastAPI(title="Fake HubSpot CRM", version="0.1.0", docs_url="/_admin/docs")
    app.state.hub = hub

    def run(
        request: Request,
        op: str,
        fn: Callable[[], Resp],
        body: dict[str, Any] | None = None,
        *,
        write: bool = False,
        search: bool = False,
    ) -> Response:
        def guarded() -> Resp:
            try:
                return fn()
            except ApiError as exc:
                return exc.resp

        return _to_response(
            hub.call(op, request.headers, guarded, write=write, search=search, body=body)
        )

    @app.post("/crm/v3/objects/{obj}/batch/{action}")
    async def batch(obj: str, action: str, request: Request) -> Response:
        body = await _json(request)
        handlers: dict[str, Callable[[], Resp]] = {
            "read": lambda: hub.batch_read(obj, body),
            "upsert": lambda: hub.batch_upsert(obj, body),
            "update": lambda: hub.batch_update(obj, body),
            "archive": lambda: hub.batch_archive(obj, body),
        }
        if action not in handlers:
            return _to_response(Resp(404, {"status": "error", "message": "Not Found"}))
        return run(
            request, f"{obj} batch/{action}", handlers[action], body, write=action in WRITE_ACTIONS
        )

    @app.post("/crm/v3/objects/{obj}/search")
    async def search(obj: str, request: Request) -> Response:
        body = await _json(request)
        return run(request, f"{obj} search", lambda: hub.search(obj, body), body, search=True)

    @app.post("/crm/v3/objects/{obj}")
    async def create(obj: str, request: Request) -> Response:
        body = await _json(request)
        return run(request, f"{obj} create", lambda: hub.create(obj, body), body, write=True)

    @app.get("/crm/v3/objects/{obj}/{ident}")
    async def get(obj: str, ident: str, request: Request) -> Response:
        q = request.query_params
        props = q.get("properties")
        return run(
            request,
            f"{obj} get",
            lambda: hub.get(obj, ident, q.get("idProperty"), props.split(",") if props else None),
        )

    @app.patch("/crm/v3/objects/{obj}/{ident}")
    async def patch(obj: str, ident: str, request: Request) -> Response:
        body = await _json(request)
        idp = request.query_params.get("idProperty")
        return run(
            request, f"{obj} patch", lambda: hub.patch(obj, ident, idp, body), body, write=True
        )

    @app.delete("/crm/v3/objects/{obj}/{ident}")
    async def delete(obj: str, ident: str, request: Request) -> Response:
        return run(request, f"{obj} delete", lambda: hub.delete(obj, ident), write=True)

    @app.post("/crm/v4/associations/{from_obj}/{to_obj}/batch/associate/default")
    async def associate(from_obj: str, to_obj: str, request: Request) -> Response:
        body = await _json(request)
        return run(
            request,
            "assoc batch/associate",
            lambda: hub.associate(from_obj, to_obj, body),
            body,
            write=True,
        )

    @app.post("/crm/v4/associations/{from_obj}/{to_obj}/batch/archive")
    async def disassociate(from_obj: str, to_obj: str, request: Request) -> Response:
        body = await _json(request)
        return run(
            request,
            "assoc batch/archive",
            lambda: hub.disassociate(from_obj, to_obj, body),
            body,
            write=True,
        )

    @app.post("/crm/v4/associations/{from_obj}/{to_obj}/batch/read")
    async def read_assocs(from_obj: str, to_obj: str, request: Request) -> Response:
        body = await _json(request)
        return run(
            request, "assoc batch/read", lambda: hub.read_assocs(from_obj, to_obj, body), body
        )

    @app.put("/crm/v4/objects/{from_obj}/{fid}/associations/default/{to_obj}/{tid}")
    async def associate_one(
        from_obj: str, fid: str, to_obj: str, tid: str, request: Request
    ) -> Response:
        return run(
            request,
            "assoc single",
            lambda: hub.associate_one(from_obj, fid, to_obj, tid),
            write=True,
        )

    # -- admin ------------------------------------------------------------------------------
    @app.get("/_admin/stats")
    async def stats() -> dict[str, Any]:
        return hub.snapshot_stats()

    @app.post("/_admin/faults")
    async def faults(request: Request) -> dict[str, Any]:
        hub.configure(**(await _json(request)))
        return hub.snapshot_stats()

    @app.post("/_admin/reset")
    async def reset() -> dict[str, Any]:
        hub.reset()
        return hub.snapshot_stats()

    @app.post("/_admin/objects/{obj}/{rid}/edit")
    async def admin_edit(obj: str, rid: str, request: Request) -> Response:
        body = await _json(request)
        try:
            return JSONResponse(hub.admin_edit(obj, rid, body.get("properties", {})))
        except ApiError as exc:
            return _to_response(exc.resp)

    @app.get("/_admin/objects/{obj}")
    async def dump(obj: str) -> list[dict[str, Any]]:
        return [hub.record_json(r, None) for r in hub.live(obj)]

    return app

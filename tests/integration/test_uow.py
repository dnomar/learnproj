from src.allocation.service_layer import unit_of_work
from src.allocation.domain import model
import uuid

def random_suffix():
    return uuid.uuid4().hex[:6]

def random_sku(name=''):
    return f'sku-{name}-{random_suffix()}'

def random_batchref(name=''):
    return f'batch-{name}-{random_suffix()}'

def random_orderid(name=''):
    return f'order-{name}-{random_suffix()}'

def insert_batch(session, ref, sku, qty, eta):
    session.execute(
        'INSERT INTO products (sku)'
        ' VALUES (:sku)',
        dict(sku=sku)
    )
    session.execute(
        'INSERT INTO batches (reference, sku, _purchased_quantity, eta)'
        ' VALUES (:ref, :sku, :qty, :eta)',
        dict(ref=ref, sku=sku, qty=qty, eta=eta)
    )


def get_allocated_batch_ref(session, orderid, sku):
    [[orderlineid]] = session.execute(
        'SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku',
        dict(orderid=orderid, sku=sku)
    )
    [[batchref]] = session.execute(
        'SELECT b.reference FROM allocations JOIN batches AS b ON batch_id = b.id'
        ' WHERE orderline_id=:orderlineid',
        dict(orderlineid=orderlineid)
    )
    return batchref


def test_uow_can_retrieve_a_batch_and_allocate_to_it(postgres_session):
    batch_ref = random_batchref("2020")
    orderline_id = random_orderid("2020")
    sku = random_sku("HIPSTER-WORKBENCH")
    session = postgres_session
    insert_batch(session, batch_ref, sku, 100, None)
    session.commit()
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    with uow:
        product = uow.products.get(sku=sku)
        line = model.OrderLine(orderline_id, sku, 10)
        product.allocate(line)
        uow.commit()
    batchref = get_allocated_batch_ref(session, orderline_id, sku)
    assert batchref == batch_ref


def test_rolls_back_uncommitted_work_by_default(postgres_session):
    batch_ref = random_batchref("2021")
    sku = random_sku("MEDIUM-PLINTH")
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    with uow:
        insert_batch(uow.session, batch_ref, sku, 100, None)

    new_session = postgres_session
    rows = list(new_session.execute("SELECT * FROM batches WHERE SKU = 'sku'"))
    assert rows == []


def test_rolls_back_on_error(postgres_session):
    batch_ref = random_batchref("2022")
    sku = random_sku("LARGE-FORK")

    class MyException(Exception):
        pass

    uow = unit_of_work.SqlAlchemyUnitOfWork()
    with pytest.raises(MyException):
        with uow:
            insert_batch(uow.session, batch_ref, sku, 100, None)
            raise MyException()

    new_session = postgres_session
    rows = list(new_session.execute("SELECT * FROM batches where sku='"+sku+"'"))
    assert rows == []

def try_to_allocate(orderid, sku, exceptions):
    line = model.OrderLine(orderid, sku, 10)
    try:
        with unit_of_work.SqlAlchemyUnitOfWork() as uow:
            product = uow.products.get(sku=sku)
            product.allocate(line)
            time.sleep(0.2)
            uow.commit()
    except Exception as e:
        print(traceback.format_exc())
        exceptions.append(e)

def test_concurrent_updates_to_version_are_not_allowed(postgres_session_factory):
    sku, batch = random_sku(), random_batchref()
    session = postgres_session_factory()
    insert_batch(session, batch, sku, 100, eta=None, product_version=1)
    session.commit()

    order1, order2 = random_orderid(1), random_orderid(2)
    exceptions = []  # type: List[Exception]
    try_to_allocate_order1 = lambda: try_to_allocate(order1, sku, exceptions)
    try_to_allocate_order2 = lambda: try_to_allocate(order2, sku, exceptions)
    thread1 = threading.Thread(target=try_to_allocate_order1)  #(1)
    thread2 = threading.Thread(target=try_to_allocate_order2)  #(1)
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

    [[version]] = session.execute(
        "SELECT version_number FROM products WHERE sku=:sku",
        dict(sku=sku),
    )
    assert version == 2  #(2)
    [exception] = exceptions
    assert 'could not serialize access due to concurrent update' in str(exception)  #(3)

    orders = list(session.execute(
        "SELECT orderid FROM allocations"
        " JOIN batches ON allocations.batch_id = batches.id"
        " JOIN order_lines ON allocations.orderline_id = order_lines.id"
        " WHERE order_lines.sku=:sku",
        dict(sku=sku),
    ))
    assert len(orders) == 1  #(4)
    with unit_of_work.SqlAlchemyUnitOfWork() as uow:
        uow.session.execute('select 1')

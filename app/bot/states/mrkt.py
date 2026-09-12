from aiogram.fsm.state import State, StatesGroup


class MrktJobStates(StatesGroup):
    awaiting_account = State()
    awaiting_inventory_retry = State()
    awaiting_gift = State()
    awaiting_price = State()
    awaiting_confirmation = State()

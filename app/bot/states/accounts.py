from aiogram.fsm.state import State, StatesGroup


class AccountStates(StatesGroup):
    viewing = State()
    awaiting_removal = State()

from aiogram.fsm.state import State, StatesGroup


class MrktTransferStates(StatesGroup):
    choosing_target = State()
    choosing_owner = State()
    choosing_asset = State()
    entering_amount = State()
    confirming = State()


class PortalsTransferStates(StatesGroup):
    choosing_target = State()
    choosing_owner = State()
    choosing_asset = State()


class TonnelTransferStates(StatesGroup):
    choosing_target = State()
    choosing_owner = State()
    choosing_asset = State()
    entering_amount = State()
    confirming = State()

from aiogram.fsm.state import State, StatesGroup


class PortalsJobStates(StatesGroup):
    awaiting_account = State()
    awaiting_offers_retry = State()
    awaiting_offer = State()
    awaiting_confirmation = State()


class PortalsOfferManagementStates(StatesGroup):
    choosing_account = State()
    viewing_offers = State()
    confirming_cancel = State()

import logging
import os
import uuid
import qrcode
import aiohttp
import re
import aiohttp
import json
import base64
import asyncio
import hashlib

from urllib.parse import urlencode
from hmac import compare_digest
from functools import wraps
from yookassa import Payment
from io import BytesIO
from datetime import datetime, timedelta
from aiosend import CryptoPay, TESTNET
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Optional

from pytonconnect import TonConnect
from pytonconnect.exceptions import UserRejectsError
from aiogram import Bot, Router, F, types, html
from aiogram.types import BufferedInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import Command, CommandObject, CommandStart, StateFilter
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.enums import ChatMemberStatus
from aiogram.utils.keyboard import InlineKeyboardBuilder

from shop_bot.bot import keyboards
from shop_bot.modules import MarzbanAPI  # ИЗМЕНЕНО: вместо xui_api
from shop_bot.data_manager.database import (
    get_user, add_new_key, get_user_keys, update_user_stats,
    register_user_if_not_exists, get_next_key_number, get_key_by_id,
    update_key_info, set_trial_used, set_terms_agreed, get_setting, get_all_hosts,
    get_plans_for_host, get_plan_by_id, log_transaction, get_referral_count,
    create_pending_transaction, get_all_users,
    create_support_ticket, add_support_message, get_user_tickets,
    get_ticket, get_ticket_messages, set_ticket_status, update_ticket_thread_info,
    get_ticket_by_thread,
    update_key_host_and_info,
    get_balance, deduct_from_balance,
    get_key_by_email, add_to_balance,
    add_to_referral_balance_all, get_referral_balance_all,
    get_referral_balance,
    is_admin,
    set_referral_start_bonus_received,
    find_and_complete_pending_transaction,
    check_promo_code_available,
    redeem_promo_code,
    update_promo_code_status,
    get_admin_ids,
)
from shop_bot.config import (
    CHOOSE_PLAN_MESSAGE,
    CHOOSE_PAYMENT_METHOD_MESSAGE,
    VPN_INACTIVE_TEXT,
    VPN_NO_DATA_TEXT,
    get_profile_text,
    get_vpn_active_text,
    get_key_info_text,
    get_purchase_success_text,
)

TELEGRAM_BOT_USERNAME = get_setting("telegram_bot_username")
PAYMENT_METHODS: dict = {}
ADMIN_ID = int(get_setting("admin_id")) if get_setting("admin_id") else None
CRYPTO_BOT_TOKEN = get_setting("cryptobot_token")

logger = logging.getLogger(__name__)

# НОВАЯ ФУНКЦИЯ: Получение клиента Marzban
def get_marzban_client():
    """Получить клиент Marzban API"""
    return MarzbanAPI(
        base_url=get_setting("marzban_url") or "http://87.242.86.245:8000/api",
        username=get_setting("marzban_username") or "admin",
        password=get_setting("marzban_password") or "admin123"
    )


class KeyPurchase(StatesGroup):
    waiting_for_host_selection = State()
    waiting_for_plan_selection = State()

class Onboarding(StatesGroup):
    waiting_for_subscription_and_agreement = State()

class PaymentProcess(StatesGroup):
    waiting_for_email = State()
    waiting_for_payment_method = State()
    waiting_for_promo_code = State()
    waiting_for_cryptobot_payment = State()

 
class TopUpProcess(StatesGroup):
    waiting_for_amount = State()
    waiting_for_topup_method = State()
    waiting_for_cryptobot_topup_payment = State()


class SupportDialog(StatesGroup):
    waiting_for_subject = State()
    waiting_for_message = State()
    waiting_for_reply = State()

def is_valid_email(email: str) -> bool:
    pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(pattern, email) is not None

async def show_main_menu(message: types.Message, edit_message: bool = False):
    user_id = message.chat.id
    user_db_data = get_user(user_id)
    user_keys = get_user_keys(user_id)
    
    trial_available = not (user_db_data and user_db_data.get('trial_used'))
    is_admin_flag = is_admin(user_id)

    custom_main_text = get_setting("main_menu_text")
    text = (custom_main_text or "🏠 <b>Главное меню</b>\n\nВыберите действие:")
    keyboard = keyboards.create_main_menu_keyboard(user_keys, trial_available, is_admin_flag)
    if edit_message:
        try:
            await message.edit_text(text, reply_markup=keyboard)
        except TelegramBadRequest:
            pass
    else:
        await message.answer(text, reply_markup=keyboard)

async def process_successful_onboarding(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try:
        set_terms_agreed(user_id)
    except Exception as e:
        logger.error(f"Не удалось установить согласие с условиями для пользователя {user_id}: {e}")
    try:
        await callback.answer()
    except Exception:
        pass
    try:
        await show_main_menu(callback.message, edit_message=True)
    except Exception:
        try:
            await callback.message.answer("✅ Требования выполнены. Открываю меню...")
        except Exception:
            pass
    try:
        await state.clear()
    except Exception:
        pass

def registration_required(f):
    @wraps(f)
    async def decorated_function(event: types.Update, *args, **kwargs):
        user_id = event.from_user.id
        user_data = get_user(user_id)
        if user_data:
            return await f(event, *args, **kwargs)
        else:
            message_text = "Пожалуйста, для начала работы со мной, отправьте команду /start"
            if isinstance(event, types.CallbackQuery):
                await event.answer(message_text, show_alert=True)
            else:
                await event.answer(message_text)
    return decorated_function

def get_user_router() -> Router:
    user_router = Router()

    def _get_stars_rate() -> Decimal:
        try:
            rate_raw = get_setting("stars_per_rub") or "1"
            rate = Decimal(str(rate_raw))
            if rate <= 0:
                rate = Decimal("1")
            return rate
        except Exception:
            return Decimal("1")

    def _calc_stars_amount(amount_rub: Decimal) -> int:
        rate = _get_stars_rate()
        try:
            stars = (amount_rub * rate).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        except Exception:
            stars = (amount_rub * rate)
        try:
            return int(stars)
        except Exception:
            return int(float(stars))
                @user_router.message(CommandStart())
    async def start_handler(message: types.Message, state: FSMContext, bot: Bot, command: CommandObject):
        user_id = message.from_user.id
        username = message.from_user.username or message.from_user.full_name
        referrer_id = None

        if command.args and command.args.startswith('ref_'):
            try:
                potential_referrer_id = int(command.args.split('_')[1])
                if potential_referrer_id != user_id:
                    referrer_id = potential_referrer_id
                    logger.info(f"Новый пользователь {user_id} был приглашен пользователем {referrer_id}")
            except (IndexError, ValueError):
                logger.warning(f"Получен некорректный реферальный код: {command.args}")
                
        register_user_if_not_exists(user_id, username, referrer_id)
        user_data = get_user(user_id)

        try:
            reward_type = (get_setting("referral_reward_type") or "percent_purchase").strip()
        except Exception:
            reward_type = "percent_purchase"
        if reward_type == "fixed_start_referrer" and referrer_id and user_data and not user_data.get('referral_start_bonus_received'):
            try:
                amount_raw = get_setting("referral_on_start_referrer_amount") or "20"
                start_bonus = Decimal(str(amount_raw)).quantize(Decimal("0.01"))
            except Exception:
                start_bonus = Decimal("20.00")
            if start_bonus > 0:
                try:
                    ok = add_to_balance(int(referrer_id), float(start_bonus))
                except Exception as e:
                    logger.warning(f"Реферальный стартовый бонус: не удалось добавить к балансу для реферера {referrer_id}: {e}")
                    ok = False
                try:
                    add_to_referral_balance_all(int(referrer_id), float(start_bonus))
                except Exception as e:
                    logger.warning(f"Реферальный стартовый бонус: не удалось увеличить общий реферальный баланс для {referrer_id}: {e}")
                try:
                    set_referral_start_bonus_received(user_id)
                except Exception:
                    pass
                try:
                    await bot.send_message(
                        chat_id=int(referrer_id),
                        text=(
                            "🎁 Начисление за приглашение!\n"
                            f"Новый пользователь: {message.from_user.full_name} (ID: {user_id})\n"
                            f"Бонус: {float(start_bonus):.2f} RUB"
                        )
                    )
                except Exception:
                    pass

        if user_data and user_data.get('agreed_to_terms'):
            await message.answer(
                f"👋 Снова здравствуйте, {html.bold(message.from_user.full_name)}!",
                reply_markup=keyboards.main_reply_keyboard
            )
            await show_main_menu(message)
            return

        terms_url = get_setting("terms_url")
        privacy_url = get_setting("privacy_url")
        channel_url = get_setting("channel_url")

        if not channel_url and (not terms_url or not privacy_url):
            set_terms_agreed(user_id)
            await show_main_menu(message)
            return

        is_subscription_forced = get_setting("force_subscription") == "true"
        
        show_welcome_screen = (is_subscription_forced and channel_url) or (terms_url and privacy_url)

        if not show_welcome_screen:
            set_terms_agreed(user_id)
            await show_main_menu(message)
            return

        welcome_parts = ["<b>Добро пожаловать!</b>\n"]
        
        if is_subscription_forced and channel_url:
            welcome_parts.append("Для доступа ко всем функциям, пожалуйста, подпишитесь на наш канал.")
        
        if terms_url and privacy_url:
            welcome_parts.append(
                "Также необходимо ознакомиться и принять наши "
                f"<a href='{terms_url}'>Условия использования</a> и "
                f"<a href='{privacy_url}'>Политику конфиденциальности</a>."
            )
        
        welcome_parts.append("\nПосле этого нажмите кнопку ниже.")
        final_text = "\n".join(welcome_parts)
        
        await message.answer(
            final_text,
            reply_markup=keyboards.create_welcome_keyboard(
                channel_url=channel_url,
                is_subscription_forced=is_subscription_forced
            ),
            disable_web_page_preview=True
        )
        await state.set_state(Onboarding.waiting_for_subscription_and_agreement)

    @user_router.callback_query(Onboarding.waiting_for_subscription_and_agreement, F.data == "check_subscription_and_agree")
    async def check_subscription_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        user_id = callback.from_user.id
        channel_url = get_setting("channel_url")
        is_subscription_forced = get_setting("force_subscription") == "true"

        if not is_subscription_forced or not channel_url:
            await process_successful_onboarding(callback, state)
            return
            
        try:
            if '@' not in channel_url and 't.me/' not in channel_url:
                logger.error(f"Неверный формат URL канала: {channel_url}. Пропускаем проверку подписки.")
                await process_successful_onboarding(callback, state)
                return

            channel_id = '@' + channel_url.split('/')[-1] if 't.me/' in channel_url else channel_url
            member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            
            if member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]:
                await process_successful_onboarding(callback, state)
            else:
                await callback.answer("Вы еще не подписались на канал. Пожалуйста, подпишитесь и попробуйте снова.", show_alert=True)

        except Exception as e:
            logger.error(f"Ошибка при проверке подписки для user_id {user_id} на канал {channel_url}: {e}")
            await callback.answer("Не удалось проверить подписку. Убедитесь, что бот является администратором канала. Попробуйте позже.", show_alert=True)

    @user_router.message(Onboarding.waiting_for_subscription_and_agreement)
    async def onboarding_fallback_handler(message: types.Message):
        await message.answer("Пожалуйста, выполните требуемые действия и нажмите на кнопку в сообщении выше.")

    @user_router.message(F.text == "🏠 Главное меню")
    @registration_required
    async def main_menu_handler(message: types.Message):
        await show_main_menu(message)

    @user_router.callback_query(F.data == "back_to_main_menu")
    @registration_required
    async def back_to_main_menu_handler(callback: types.CallbackQuery):
        await callback.answer()
        await show_main_menu(callback.message, edit_message=True)

    @user_router.callback_query(F.data == "show_main_menu")
    @registration_required
    async def show_main_menu_cb(callback: types.CallbackQuery):
        await callback.answer()
        await show_main_menu(callback.message, edit_message=True)

    @user_router.callback_query(F.data == "show_profile")
    @registration_required
    async def profile_handler_callback(callback: types.CallbackQuery):
        await callback.answer()
        user_id = callback.from_user.id
        user_db_data = get_user(user_id)
        user_keys = get_user_keys(user_id)
        if not user_db_data:
            await callback.answer("Не удалось получить данные профиля.", show_alert=True)
            return
        username = html.bold(user_db_data.get('username', 'Пользователь'))
        total_spent, total_months = user_db_data.get('total_spent', 0), user_db_data.get('total_months', 0)
        now = datetime.now()
        active_keys = [key for key in user_keys if datetime.fromisoformat(key['expiry_date']) > now]
        if active_keys:
            latest_key = max(active_keys, key=lambda k: datetime.fromisoformat(k['expiry_date']))
            latest_expiry_date = datetime.fromisoformat(latest_key['expiry_date'])
            time_left = latest_expiry_date - now
            vpn_status_text = get_vpn_active_text(time_left.days, time_left.seconds // 3600)
        elif user_keys: vpn_status_text = VPN_INACTIVE_TEXT
        else: vpn_status_text = VPN_NO_DATA_TEXT
        final_text = get_profile_text(username, total_spent, total_months, vpn_status_text)
        try:
            main_balance = get_balance(user_id)
        except Exception:
            main_balance = 0.0
        final_text += f"\n\n💼 <b>Основной баланс:</b> {main_balance:.0f} RUB"
        try:
            referral_count = get_referral_count(user_id)
        except Exception:
            referral_count = 0
        try:
            total_ref_earned = float(get_referral_balance_all(user_id))
        except Exception:
            total_ref_earned = 0.0
        final_text += (
            f"\n🤝 <b>Рефералы:</b> {referral_count}"
            f"\n💰 <b>Заработано по рефералке (всего):</b> {total_ref_earned:.2f} RUB"
        )
        await callback.message.edit_text(final_text, reply_markup=keyboards.create_profile_keyboard())

    @user_router.callback_query(F.data == "profile_info")
    @registration_required
    async def profile_info_handler(callback: types.CallbackQuery):
        await callback.answer()
        user_id = callback.from_user.id
        user_db_data = get_user(user_id)
        user_keys = get_user_keys(user_id)
        
        if not user_db_data:
            await callback.answer("Не удалось получить данные профиля.", show_alert=True)
            return
            
        username = html.bold(user_db_data.get('username', 'Пользователь'))
        total_spent = user_db_data.get('total_spent', 0)
        total_months = user_db_data.get('total_months', 0)
        
        now = datetime.now()
        active_keys = [key for key in user_keys if datetime.fromisoformat(key['expiry_date']) > now]
        
        if active_keys:
            latest_key = max(active_keys, key=lambda k: datetime.fromisoformat(k['expiry_date']))
            latest_expiry_date = datetime.fromisoformat(latest_key['expiry_date'])
            time_left = latest_expiry_date - now
            vpn_status_text = get_vpn_active_text(time_left.days, time_left.seconds // 3600)
        elif user_keys:
            vpn_status_text = VPN_INACTIVE_TEXT
        else:
            vpn_status_text = VPN_NO_DATA_TEXT
            
        final_text = get_profile_text(username, total_spent, total_months, vpn_status_text)
        
        final_text += f"\n\n📊 <b>Статистика:</b>"
        final_text += f"\n🔑 <b>Всего ключей:</b> {len(user_keys)}"
        final_text += f"\n✅ <b>Активных ключей:</b> {len(active_keys)}"
        final_text += f"\n💸 <b>Потрачено всего:</b> {total_spent:.2f} RUB"
        final_text += f"\n📅 <b>Месяцев подписки:</b> {total_months}"
        
        builder = InlineKeyboardBuilder()
        builder.button(text="⬅️ Назад", callback_data="show_profile")
        await callback.message.edit_text(final_text, reply_markup=builder.as_markup())

    @user_router.callback_query(F.data == "profile_balance")
    @registration_required
    async def profile_balance_handler(callback: types.CallbackQuery):
        await callback.answer()
        user_id = callback.from_user.id
        
        try:
            main_balance = get_balance(user_id)
        except Exception:
            main_balance = 0.0
            
        try:
            referral_count = get_referral_count(user_id)
        except Exception:
            referral_count = 0
            
        try:
            total_ref_earned = float(get_referral_balance_all(user_id))
        except Exception:
            total_ref_earned = 0.0
            
        try:
            ref_balance = float(get_referral_balance(user_id))
        except Exception:
            ref_balance = 0.0
        
        text = f"💰 <b>Информация о балансе</b>\n\n"
        text += f"💼 <b>Основной баланс:</b> {main_balance:.2f} RUB\n"
        text += f"🤝 <b>Реферальный баланс:</b> {ref_balance:.2f} RUB\n"
        text += f"📊 <b>Всего заработано по рефералке:</b> {total_ref_earned:.2f} RUB\n"
        text += f"👥 <b>Приглашено пользователей:</b> {referral_count}\n\n"
        text += f"💡 <b>Совет:</b> Используйте реферальный баланс для покупки ключей!"
        
        builder = InlineKeyboardBuilder()
        builder.button(text="💳 Пополнить", callback_data="top_up_start")
        builder.button(text="⬅️ Назад", callback_data="show_profile")
        builder.adjust(1)
        await callback.message.edit_text(text, reply_markup=builder.as_markup())

    @user_router.callback_query(F.data == "main_menu")
    @registration_required
    async def profile_main_menu_handler(callback: types.CallbackQuery):
        await callback.answer()
        await show_main_menu(callback.message, edit_message=True)

    @user_router.callback_query(F.data == "top_up_start")
    @registration_required
    async def topup_start_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await callback.message.edit_text(
            "Введите сумму пополнения в рублях (например, 300):\nМинимум: 10 RUB, максимум: 100000 RUB",
        )
        await state.set_state(TopUpProcess.waiting_for_amount)
            @user_router.callback_query(F.data == "manage_keys")
    @registration_required
    async def manage_keys_handler(callback: types.CallbackQuery):
        await callback.answer()
        user_id = callback.from_user.id
        user_keys = get_user_keys(user_id)
        await callback.message.edit_text(
            "Ваши ключи:" if user_keys else "У вас пока нет ключей.",
            reply_markup=keyboards.create_keys_management_keyboard(user_keys)
        )

    @user_router.callback_query(F.data == "get_trial")
    @registration_required
    async def trial_period_handler(callback: types.CallbackQuery, state: FSMContext):
        user_id = callback.from_user.id
        user_db_data = get_user(user_id)
        if user_db_data and user_db_data.get('trial_used'):
            await callback.answer("Вы уже использовали бесплатный пробный период.", show_alert=True)
            return

        hosts = get_all_hosts()
        if not hosts:
            await callback.message.edit_text("❌ В данный момент нет доступных серверов для создания пробного ключа.")
            return
            
        if len(hosts) == 1:
            await callback.answer()
            await process_trial_key_creation(callback.message, hosts[0]['host_name'])
        else:
            await callback.answer()
            await callback.message.edit_text(
                "Выберите сервер, на котором хотите получить пробный ключ:",
                reply_markup=keyboards.create_host_selection_keyboard(hosts, action="trial")
            )

    # ИСПРАВЛЕННАЯ ФУНКЦИЯ: process_trial_key_creation (Marzban)
    async def process_trial_key_creation(message: types.Message, host_name: str):
        user_id = message.chat.id
        await message.edit_text(f"Отлично! Создаю для вас бесплатный ключ на {get_setting('trial_duration_days')} дня...")

        try:
            marzban = get_marzban_client()
            
            user_data = get_user(user_id) or {}
            raw_username = (user_data.get('username') or f'user{user_id}').lower()
            username_slug = re.sub(r"[^a-z0-9._-]", "_", raw_username).strip("_")[:16] or f"user{user_id}"
            marzban_username = f"trial_{username_slug}_{int(datetime.now().timestamp())}"
            
            result = await marzban.create_user(
                username=marzban_username,
                expire_days=int(get_setting("trial_duration_days")),
                data_limit_gb=0
            )
            
            if not result:
                await message.edit_text("❌ Не удалось создать пробный ключ.")
                return

            set_trial_used(user_id)
            
            subscription_link = await marzban.get_subscription_link(marzban_username)
            expiry_timestamp_ms = result.get('expire', 0) * 1000
            
            new_key_id = add_new_key(
                user_id=user_id,
                host_name=host_name,
                xui_client_uuid=marzban_username,
                key_email=f"{marzban_username}@marzban.local",
                expiry_timestamp_ms=expiry_timestamp_ms
            )
            
            new_expiry_date = datetime.fromtimestamp(expiry_timestamp_ms / 1000)
            final_text = get_purchase_success_text("готов", get_next_key_number(user_id) - 1, new_expiry_date, subscription_link)
            
            try:
                await message.edit_text(text=final_text, reply_markup=keyboards.create_key_info_keyboard(new_key_id), disable_web_page_preview=True)
            except TelegramBadRequest:
                try:
                    await message.delete()
                except Exception:
                    pass
                await message.answer(text=final_text, reply_markup=keyboards.create_key_info_keyboard(new_key_id))

        except Exception as e:
            logger.error(f"Ошибка создания пробного ключа для пользователя {user_id}: {e}", exc_info=True)
            await message.edit_text("❌ Произошла ошибка при создании пробного ключа.")

    # ИСПРАВЛЕННАЯ ФУНКЦИЯ: show_key_handler (Marzban)
    @user_router.callback_query(F.data.startswith("show_key_"))
    @registration_required
    async def show_key_handler(callback: types.CallbackQuery):
        key_id_to_show = int(callback.data.split("_")[2])
        await callback.message.edit_text("Загружаю информацию о ключе...")
        user_id = callback.from_user.id
        key_data = get_key_by_id(key_id_to_show)

        if not key_data or key_data['user_id'] != user_id:
            await callback.message.edit_text("❌ Ошибка: ключ не найден.")
            return
            
        try:
            marzban = get_marzban_client()
            marzban_username = key_data.get('xui_client_uuid')
            
            user_info = await marzban.get_user(marzban_username)
            if not user_info:
                await callback.message.edit_text("❌ Не удалось получить данные ключа.")
                return
            
            subscription_link = await marzban.get_subscription_link(marzban_username)
            expiry_date = datetime.fromtimestamp(user_info.get('expire', 0))
            created_date = datetime.fromisoformat(key_data['created_date'])
            
            all_user_keys = get_user_keys(user_id)
            key_number = next((i + 1 for i, key in enumerate(all_user_keys) if key['key_id'] == key_id_to_show), 0)
            
            final_text = get_key_info_text(key_number, expiry_date, created_date, subscription_link)
            
            await callback.message.edit_text(
                text=final_text,
                reply_markup=keyboards.create_key_info_keyboard(key_id_to_show)
            )
        except Exception as e:
            logger.error(f"Ошибка показа ключа {key_id_to_show}: {e}")
            await callback.message.edit_text("❌ Произошла ошибка при получении данных ключа.")

    @user_router.callback_query(F.data.startswith("switch_server_"))
    @registration_required
    async def switch_server_start(callback: types.CallbackQuery):
        await callback.answer()
        try:
            key_id = int(callback.data[len("switch_server_"):])
        except ValueError:
            await callback.answer("Некорректный идентификатор ключа.", show_alert=True)
            return

        key_data = get_key_by_id(key_id)
        if not key_data or key_data.get('user_id') != callback.from_user.id:
            await callback.answer("Ключ не найден.", show_alert=True)
            return

        hosts = get_all_hosts()
        if not hosts:
            await callback.answer("Нет доступных серверов.", show_alert=True)
            return

        current_host = key_data.get('host_name')
        hosts = [h for h in hosts if h.get('host_name') != current_host]
        if not hosts:
            await callback.answer("Другие серверы отсутствуют.", show_alert=True)
            return

        await callback.message.edit_text(
            "Выберите новый сервер (локацию) для этого ключа:",
            reply_markup=keyboards.create_host_selection_keyboard(hosts, action=f"switch_{key_id}")
        )

    # ИСПРАВЛЕННАЯ ФУНКЦИЯ: _switch_key_to_host (Marzban)
    async def _switch_key_to_host(callback: types.CallbackQuery, key_id: int, new_host_name: str):
        key_data = get_key_by_id(key_id)

        if not key_data or key_data.get('user_id') != callback.from_user.id:
            await callback.answer("Ключ не найден.", show_alert=True)
            return

        old_host = key_data.get('host_name')
        if not old_host:
            await callback.answer("Для ключа не указан текущий сервер.", show_alert=True)
            return

        if new_host_name == old_host:
            await callback.answer("Это уже текущий сервер.", show_alert=True)
            return

        await callback.answer()
        await callback.message.edit_text(f"⏳ Переношу ключ на сервер \"{new_host_name}\"...")

        try:
            marzban = get_marzban_client()
            marzban_username = key_data.get('xui_client_uuid')
            
            user_info = await marzban.get_user(marzban_username)
            if not user_info:
                await callback.message.edit_text("❌ Не удалось найти пользователя в Marzban.")
                return
            
            # Обновляем запись в БД (меняем хост)
            update_key_host_and_info(
                key_id=key_id,
                new_host_name=new_host_name,
                new_xui_uuid=marzban_username,
                new_expiry_ms=user_info.get('expire', 0) * 1000
            )
            
            subscription_link = await marzban.get_subscription_link(marzban_username)
            
            updated_key = get_key_by_id(key_id)
            expiry_date = datetime.fromtimestamp(user_info.get('expire', 0))
            created_date = datetime.fromisoformat(updated_key['created_date'])
            all_user_keys = get_user_keys(callback.from_user.id)
            key_number = next((i + 1 for i, k in enumerate(all_user_keys) if k['key_id'] == key_id), 0)
            final_text = get_key_info_text(key_number, expiry_date, created_date, subscription_link)
            
            await callback.message.edit_text(
                text=final_text,
                reply_markup=keyboards.create_key_info_keyboard(key_id)
            )
            
        except Exception as e:
            logger.error(f"Ошибка переключения ключа {key_id} на хост {new_host_name}: {e}", exc_info=True)
            await callback.message.edit_text("❌ Произошла ошибка при переносе ключа.")

    @user_router.callback_query(F.data.startswith("select_host_switch_"))
    @registration_required
    async def select_host_for_switch(callback: types.CallbackQuery):
        payload = callback.data[len("select_host_switch_"):]
        parts = payload.split("_", 1)
        if len(parts) != 2:
            await callback.answer("Некорректные данные выбора сервера.", show_alert=True)
            return
        try:
            key_id = int(parts[0])
        except ValueError:
            await callback.answer("Некорректный идентификатор ключа.", show_alert=True)
            return
        new_host_name = parts[1]
        await _switch_key_to_host(callback, key_id, new_host_name)

    async def handle_switch_host(callback: types.CallbackQuery, key_id: int, new_host_name: str):
        await _switch_key_to_host(callback, key_id, new_host_name)

    @user_router.callback_query(F.data.startswith("show_qr_"))
    @registration_required
    async def show_qr_handler(callback: types.CallbackQuery):
        await callback.answer("Генерирую QR-код...")
        key_id = int(callback.data.split("_")[2])
        key_data = get_key_by_id(key_id)
        if not key_data or key_data['user_id'] != callback.from_user.id: return
        
        try:
            marzban = get_marzban_client()
            marzban_username = key_data.get('xui_client_uuid')
            subscription_link = await marzban.get_subscription_link(marzban_username)
            
            qr_img = qrcode.make(subscription_link)
            bio = BytesIO()
            qr_img.save(bio, "PNG")
            bio.seek(0)
            qr_code_file = BufferedInputFile(bio.read(), filename="vpn_qr.png")
            await callback.message.answer_photo(photo=qr_code_file)
        except Exception as e:
            logger.error(f"Ошибка показа QR-кода для ключа {key_id}: {e}")

    @user_router.callback_query(F.data.startswith("howto_vless_"))
    @registration_required
    async def show_instruction_handler(callback: types.CallbackQuery):
        await callback.answer()
        key_id = int(callback.data.split("_")[2])
        try:
            await callback.message.edit_text(
                "Выберите вашу платформу для инструкции по подключению VLESS:",
                reply_markup=keyboards.create_howto_vless_keyboard_key(key_id),
                disable_web_page_preview=True
            )
        except TelegramBadRequest:
            pass
    
    @user_router.callback_query(F.data.startswith("howto_vless"))
    @registration_required
    async def show_instruction_handler(callback: types.CallbackQuery):
        await callback.answer()

        try:
            await callback.message.edit_text(
                "Выберите вашу платформу для инструкции по подключению VLESS:",
                reply_markup=keyboards.create_howto_vless_keyboard(),
                disable_web_page_preview=True
            )
        except TelegramBadRequest:
            pass

    @user_router.callback_query(F.data == "user_speedtest")
    @registration_required
    async def user_speedtest_handler(callback: types.CallbackQuery):
        await callback.answer()
        
        try:
            hosts = get_all_hosts() or []
            if not hosts:
                await callback.message.edit_text(
                    "⚠️ Хосты не найдены в настройках. Обратитесь к администратору.",
                    reply_markup=keyboards.create_back_to_main_menu_keyboard()
                )
                return
            
            text = "⚡️ <b>Последние результаты Speedtest</b>\n\n"
            
            from shop_bot.data_manager.database import get_latest_speedtest
            
            for host in hosts:
                host_name = host.get('host_name', 'Неизвестный хост')
                latest_test = get_latest_speedtest(host_name)
                
                if latest_test:
                    ping = latest_test.get('ping_ms')
                    download = latest_test.get('download_mbps')
                    upload = latest_test.get('upload_mbps')
                    server = latest_test.get('server_name', '—')
                    method = latest_test.get('method', 'unknown').upper()
                    created_at = latest_test.get('created_at', '—')
                    
                    try:
                        from datetime import datetime
                        if created_at and created_at != '—':
                            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            time_str = dt.strftime('%d.%m %H:%M')
                        else:
                            time_str = '—'
                    except:
                        time_str = created_at
                    
                    ping_str = f"{ping:.2f}" if ping is not None else "—"
                    download_str = f"{download:.0f}" if download is not None else "—"
                    upload_str = f"{upload:.0f}" if upload is not None else "—"
                    
                    text += f"• 🌏{host_name} — {method}: ✅ · ⏱️ {ping_str} ms · ↓ {download_str} Mbps · ↑ {upload_str} Mbps · 🕒 {time_str}\n"
                else:
                    text += f"• 🌏{host_name} — Нет данных о тестах скорости\n"
            
            await callback.message.edit_text(
                text,
                reply_markup=keyboards.create_back_to_main_menu_keyboard(),
                disable_web_page_preview=True
            )
        except TelegramBadRequest:
            pass
                # ИСПРАВЛЕННАЯ ФУНКЦИЯ: process_successful_payment (Marzban)
    # Это самая важная функция - замените её полностью

    async def process_successful_payment(bot: Bot, metadata: dict):
        try:
            action = metadata.get('action')
            user_id = int(metadata.get('user_id'))
            price = float(metadata.get('price'))
            months = int(metadata.get('months', 0))
            key_id = int(metadata.get('key_id', 0)) if metadata.get('key_id') is not None else 0
            host_name = metadata.get('host_name', '')
            plan_id = int(metadata.get('plan_id', 0)) if metadata.get('plan_id') is not None else 0
            customer_email = metadata.get('customer_email')
            payment_method = metadata.get('payment_method')

            chat_id_to_delete = metadata.get('chat_id')
            message_id_to_delete = metadata.get('message_id')
            
        except (ValueError, TypeError) as e:
            logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось разобрать метаданные. Ошибка: {e}. Метаданные: {metadata}")
            return

        if chat_id_to_delete and message_id_to_delete:
            try:
                await bot.delete_message(chat_id=chat_id_to_delete, message_id=message_id_to_delete)
            except TelegramBadRequest as e:
                logger.warning(f"Не удалось удалить сообщение о платеже: {e}")

        # Спец-ветка: пополнение баланса
        if action == "top_up":
            try:
                ok = add_to_balance(user_id, float(price))
            except Exception as e:
                logger.error(f"Не удалось добавить к балансу для пользователя {user_id}: {e}", exc_info=True)
                ok = False
            try:
                user_info = get_user(user_id)
                log_username = user_info.get('username', 'N/A') if user_info else 'N/A'
                log_transaction(
                    username=log_username,
                    transaction_id=None,
                    payment_id=str(uuid.uuid4()),
                    user_id=user_id,
                    status='paid',
                    amount_rub=float(price),
                    amount_currency=None,
                    currency_name=None,
                    payment_method=payment_method or 'Unknown',
                    metadata=json.dumps({"action": "top_up"})
                )
            except Exception:
                pass
            try:
                current_balance = 0.0
                try:
                    current_balance = float(get_balance(user_id))
                except Exception:
                    pass
                if ok:
                    await bot.send_message(
                        chat_id=user_id,
                        text=(
                            f"✅ Оплата получена!\n"
                            f"💼 Баланс пополнен на {float(price):.2f} RUB.\n"
                            f"Текущий баланс: {current_balance:.2f} RUB."
                        ),
                        reply_markup=keyboards.create_profile_keyboard()
                    )
                else:
                    await bot.send_message(
                        chat_id=user_id,
                        text=(
                            "⚠️ Оплата получена, но не удалось обновить баланс. "
                            "Обратитесь в поддержку."
                        ),
                        reply_markup=keyboards.create_support_keyboard()
                    )
            except Exception:
                pass
            try:
                admins = [u for u in (get_all_users() or []) if is_admin(u.get('telegram_id') or 0)]
                for a in admins:
                    admin_id = a.get('telegram_id')
                    if admin_id:
                        await bot.send_message(admin_id, f"📥 Пополнение: пользователь {user_id}, сумма {float(price):.2f} RUB")
            except Exception:
                pass
            return

        processing_message = await bot.send_message(
            chat_id=user_id,
            text=f"✅ Оплата получена! Обрабатываю ваш запрос..."
        )
        
        try:
            marzban = get_marzban_client()
            result = None
            subscription_link = None
            
            if action == "new":
                # Создание нового пользователя
                user_data = get_user(user_id) or {}
                raw_username = (user_data.get('username') or f'user{user_id}').lower()
                username_slug = re.sub(r"[^a-z0-9._-]", "_", raw_username).strip("_")[:16] or f"user{user_id}"
                base_local = f"{username_slug}"
                candidate_local = base_local
                attempt = 1
                while True:
                    marzban_username = f"{candidate_local}_{int(datetime.now().timestamp())}"
                    existing = await marzban.get_user(marzban_username)
                    if not existing:
                        break
                    attempt += 1
                    candidate_local = f"{base_local}-{attempt}"
                    if attempt > 100:
                        marzban_username = f"{base_local}_{int(datetime.now().timestamp())}"
                        break
                
                user_result = await marzban.create_user(
                    username=marzban_username,
                    expire_days=int(months * 30),
                    data_limit_gb=0
                )
                
                if not user_result:
                    await processing_message.edit_text("❌ Не удалось создать пользователя в панели.")
                    return
                
                subscription_link = await marzban.get_subscription_link(marzban_username)
                expiry_timestamp_ms = user_result.get('expire', 0) * 1000
                
                key_id = add_new_key(
                    user_id=user_id,
                    host_name=host_name,
                    xui_client_uuid=marzban_username,
                    key_email=f"{marzban_username}@marzban.local",
                    expiry_timestamp_ms=expiry_timestamp_ms
                )
                
                result = {
                    'client_uuid': marzban_username,
                    'expiry_timestamp_ms': expiry_timestamp_ms,
                    'connection_string': subscription_link
                }
                
            elif action == "extend":
                # Продление существующего ключа
                existing_key = get_key_by_id(key_id)
                if not existing_key:
                    await processing_message.edit_text("❌ Не удалось найти ключ для продления.")
                    return
                
                marzban_username = existing_key.get('xui_client_uuid')
                if not marzban_username:
                    await processing_message.edit_text("❌ Не удалось найти пользователя в панели.")
                    return
                
                user_result = await marzban.update_user_expiry(marzban_username, int(months * 30))
                subscription_link = await marzban.get_subscription_link(marzban_username)
                expiry_timestamp_ms = user_result.get('expire', 0) * 1000
                
                update_key_info(key_id, marzban_username, expiry_timestamp_ms)
                
                result = {
                    'client_uuid': marzban_username,
                    'expiry_timestamp_ms': expiry_timestamp_ms,
                    'connection_string': subscription_link
                }
            else:
                await processing_message.edit_text("❌ Неизвестное действие.")
                return

            if not result:
                await processing_message.edit_text("❌ Не удалось обработать запрос.")
                return

            # Начисляем реферальное вознаграждение
            user_data = get_user(user_id)
            referrer_id = user_data.get('referred_by') if user_data else None
            if referrer_id:
                try:
                    referrer_id = int(referrer_id)
                except Exception:
                    logger.warning(f"Referral: invalid referrer_id={referrer_id} for user {user_id}")
                    referrer_id = None
                    
            if referrer_id:
                try:
                    reward_type = (get_setting("referral_reward_type") or "percent_purchase").strip()
                except Exception:
                    reward_type = "percent_purchase"
                reward = Decimal("0")
                if reward_type == "fixed_start_referrer":
                    reward = Decimal("0")
                elif reward_type == "fixed_purchase":
                    try:
                        amount_raw = get_setting("fixed_referral_bonus_amount") or "50"
                        reward = Decimal(str(amount_raw)).quantize(Decimal("0.01"))
                    except Exception:
                        reward = Decimal("50.00")
                else:
                    try:
                        percentage = Decimal(get_setting("referral_percentage") or "0")
                    except Exception:
                        percentage = Decimal("0")
                    reward = (Decimal(str(price)) * percentage / 100).quantize(Decimal("0.01"))
                    
                if float(reward) > 0:
                    try:
                        add_to_balance(referrer_id, float(reward))
                        add_to_referral_balance_all(referrer_id, float(reward))
                        referrer_username = user_data.get('username', 'пользователь') if user_data else 'пользователь'
                        try:
                            await bot.send_message(
                                chat_id=referrer_id,
                                text=(
                                    "💰 Вам начислено реферальное вознаграждение!\n"
                                    f"Пользователь: {referrer_username} (ID: {user_id})\n"
                                    f"Сумма: {float(reward):.2f} RUB"
                                )
                            )
                        except Exception as e:
                            logger.warning(f"Could not send referral reward notification to {referrer_id}: {e}")
                    except Exception as e:
                        logger.warning(f"Referral: failed to add reward for referrer {referrer_id}: {e}")

            # Обновляем статистику пользователя
            try:
                pm_lower = (payment_method or '').strip().lower()
            except Exception:
                pm_lower = ''
            spent_for_stats = 0.0 if pm_lower == 'balance' else float(price)
            update_user_stats(user_id, spent_for_stats, months)
            
            # Логируем транзакцию
            user_info = get_user(user_id)
            log_username = user_info.get('username', 'N/A') if user_info else 'N/A'
            log_metadata = json.dumps({
                "plan_id": plan_id,
                "plan_name": get_plan_by_id(plan_id).get('plan_name', 'Unknown') if get_plan_by_id(plan_id) else 'Unknown',
                "host_name": host_name,
                "customer_email": customer_email
            })
            payment_id_for_log = metadata.get('payment_id') or str(uuid.uuid4())
            log_transaction(
                username=log_username,
                transaction_id=None,
                payment_id=payment_id_for_log,
                user_id=user_id,
                status='paid',
                amount_rub=float(price),
                amount_currency=None,
                currency_name=None,
                payment_method=payment_method or 'Unknown',
                metadata=log_metadata
            )
            
            # Обработка промокода
            try:
                promo_code_used = (metadata.get('promo_code') or '').strip()
                if promo_code_used:
                    try:
                        applied_amt = 0.0
                        try:
                            if metadata.get('promo_discount_amount') is not None:
                                applied_amt = float(metadata.get('promo_discount_amount') or 0.0)
                        except Exception:
                            applied_amt = 0.0
                        redeemed = redeem_promo_code(
                            promo_code_used,
                            user_id,
                            applied_amount=float(applied_amt or 0.0),
                            order_id=payment_id_for_log,
                        )
                        if redeemed:
                            limit_total = redeemed.get('usage_limit_total')
                            per_user_limit = redeemed.get('usage_limit_per_user')
                            used_total_now = redeemed.get('used_total') or 0
                            user_usage_count = redeemed.get('user_usage_count')
                            should_deactivate = False
                            reason_lines = []

                            if limit_total:
                                try:
                                    if used_total_now >= int(limit_total):
                                        should_deactivate = True
                                        reason_lines.append("достигнут общий лимит использования")
                                except Exception:
                                    pass

                            if per_user_limit:
                                try:
                                    if (user_usage_count or 0) >= int(per_user_limit):
                                        should_deactivate = True
                                        reason_lines.append("исчерпан лимит на пользователя")
                                except Exception:
                                    pass

                            if not should_deactivate and (limit_total or per_user_limit):
                                should_deactivate = True
                                if per_user_limit and not reason_lines:
                                    reason_lines.append("лимит на пользователя выставлен (код погашён)")
                                elif limit_total and not reason_lines:
                                    reason_lines.append("лимит по количеству использований выставлен (код погашён)")

                            if should_deactivate:
                                try:
                                    update_promo_code_status(promo_code_used, is_active=False)
                                except Exception:
                                    pass

                            try:
                                plan = get_plan_by_id(plan_id)
                                plan_name = plan.get('plan_name', 'Unknown') if plan else 'Unknown'
                                admins = list(get_admin_ids() or [])
                                if should_deactivate:
                                    status_line = "Статус: деактивирован"
                                    if reason_lines:
                                        status_line += " (" + ", ".join(reason_lines) + ")"
                                else:
                                    status_line = "Статус: активен"
                                    if limit_total:
                                        status_line += f" (использовано {used_total_now} из {limit_total})"
                                    else:
                                        status_line += f" (использовано {used_total_now})"
                                text = (
                                    "🎟️ Промокод использован\n"
                                    f"Код: {promo_code_used}\n"
                                    f"Пользователь: {user_id}\n"
                                    f"Тариф: {plan_name} ({months} мес.)\n"
                                    f"{status_line}"
                                )
                                for aid in admins:
                                    try:
                                        await bot.send_message(int(aid), text)
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                    except Exception as e:
                        logger.warning(f"Promo redeem failed for user {user_id}, code {promo_code_used}: {e}")
            except Exception:
                pass
            
            # Удаляем служебное сообщение
            try:
                await processing_message.delete()
            except Exception:
                pass
            
            connection_string = result.get('connection_string')
            new_expiry_date = datetime.fromtimestamp(result['expiry_timestamp_ms'] / 1000)
            
            all_user_keys = get_user_keys(user_id)
            key_number = len(all_user_keys)
            
            final_text = get_purchase_success_text(
                action="создан" if action == "new" else "продлен",
                key_number=key_number,
                expiry_date=new_expiry_date,
                connection_string=connection_string or ""
            )
            
            await bot.send_message(
                chat_id=user_id,
                text=final_text,
                reply_markup=keyboards.create_key_info_keyboard(key_id)
            )
            
            try:
                await notify_admin_of_purchase(bot, metadata)
            except Exception as e:
                logger.warning(f"Failed to notify admin of purchase: {e}")
            
        except Exception as e:
            logger.error(f"Error processing payment for user {user_id}: {e}", exc_info=True)
            try:
                await processing_message.edit_text("❌ Ошибка при выдаче ключа.")
            except Exception:
                try:
                    await bot.send_message(chat_id=user_id, text="❌ Ошибка при выдаче ключа.")
                except Exception:
                    pass

    return user_router


# Вспомогательные функции (остаются без изменений)
async def _create_heleket_payment_request(
    user_id: int,
    price: float,
    months: int,
    host_name: str,
    state_data: dict,
) -> Optional[str]:
    # Этот код остается без изменений (работает с платежами, не с XUI)
    try:
        merchant_id = get_setting("heleket_merchant_id")
        api_key = get_setting("heleket_api_key")
        if not merchant_id or not api_key:
            logger.error("Heleket: отсутствуют merchant_id/api_key в настройках.")
            return None

        metadata = {
            "payment_id": str(uuid.uuid4()),
            "user_id": user_id,
            "months": months,
            "price": float(price),
            "action": state_data.get("action"),
            "key_id": state_data.get("key_id"),
            "host_name": host_name,
            "plan_id": state_data.get("plan_id"),
            "customer_email": state_data.get("customer_email"),
            "payment_method": "Crypto",
            "promo_code": state_data.get("promo_code"),
            "promo_discount_percent": state_data.get('promo_discount_percent'),
            "promo_discount_amount": state_data.get('promo_discount_amount'),
        }

        dom_val = get_setting("domain")
        domain = (dom_val or "").strip() if isinstance(dom_val, str) else dom_val
        callback_url = None
        try:
            if domain:
                callback_url = f"{str(domain).rstrip('/')}/heleket-webhook"
        except Exception:
            callback_url = None

        success_url = None
        try:
            if TELEGRAM_BOT_USERNAME:
                success_url = f"https://t.me/{TELEGRAM_BOT_USERNAME}"
        except Exception:
            success_url = None

        data: Dict[str, object] = {
            "merchant_id": merchant_id,
            "order_id": str(uuid.uuid4()),
            "amount": float(price),
            "currency": "RUB",
            "description": json.dumps(metadata, ensure_ascii=False, separators=(",", ":")),
        }
        if callback_url:
            data["callback_url"] = callback_url
        if success_url:
            data["success_url"] = success_url

        sorted_data_str = json.dumps(data, sort_keys=True, separators=(",", ":"))
        base64_encoded = base64.b64encode(sorted_data_str.encode()).decode()
        raw_string = f"{base64_encoded}{api_key}"
        sign = hashlib.md5(raw_string.encode()).hexdigest()

        payload = dict(data)
        payload["sign"] = sign

        api_base_val = get_setting("heleket_api_base")
        api_base = (api_base_val or "https://api.heleket.com").rstrip("/")
        endpoint = f"{api_base}/invoice/create"

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(endpoint, json=payload, timeout=15) as resp:
                    text = await resp.text()
                    if resp.status not in (200, 201):
                        logger.error(f"Heleket: не удалось создать счёт (HTTP {resp.status}): {text}")
                        return None
                    try:
                        data_json = await resp.json()
                    except Exception:
                        logger.warning(f"Heleket: неожиданный ответ (не JSON): {text}")
                        return None
                    pay_url = (
                        data_json.get("payment_url")
                        or data_json.get("pay_url")
                        or data_json.get("url")
                    )
                    if not pay_url:
                        logger.error(f"Heleket: не найдено поле URL в ответе: {data_json}")
                        return None
                    return str(pay_url)
            except Exception as e:
                logger.error(f"Heleket: ошибка HTTP при создании счёта: {e}", exc_info=True)
                return None
    except Exception as e:
        logger.error(f"Heleket: общая ошибка при создании счёта: {e}", exc_info=True)
        return None


async def _create_cryptobot_invoice(
    user_id: int,
    price_rub: float,
    months: int,
    host_name: str,
    state_data: dict,
) -> Optional[tuple[str, int]]:
    try:
        token = get_setting("cryptobot_token")
        if not token:
            logger.error("CryptoBot: не задан cryptobot_token")
            return None

        rate = await get_usdt_rub_rate()
        if not rate or rate <= 0:
            logger.error("CryptoBot: не удалось получить курс USDT/RUB")
            return None

        amount_usdt = (Decimal(str(price_rub)) / rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        payload_parts = [
            str(user_id),
            str(months),
            str(float(price_rub)),
            str(state_data.get("action")),
            str(state_data.get("key_id")),
            str(host_name or ""),
            str(state_data.get("plan_id")),
            str(state_data.get("customer_email")),
            "CryptoBot",
            str(state_data.get("promo_code") or ""),
        ]
        payload = ":".join(payload_parts)

        cp = CryptoPay(token)
        invoice = await cp.create_invoice(
            asset="USDT",
            amount=float(amount_usdt),
            description="VPN оплата",
            payload=payload,
        )

        pay_url = None
        invoice_id = None
        
        try:
            pay_url = getattr(invoice, "pay_url", None) or getattr(invoice, "bot_invoice_url", None)
            invoice_id = getattr(invoice, "invoice_id", None)
        except Exception:
            pass
        
        if not pay_url and isinstance(invoice, dict):
            pay_url = invoice.get("pay_url") or invoice.get("bot_invoice_url") or invoice.get("url")
            
        if not invoice_id and isinstance(invoice, dict):
            invoice_id = invoice.get("invoice_id")
            
        if not pay_url:
            logger.error(f"CryptoBot: не удалось получить ссылку на оплату из ответа: {invoice}")
            return None
            
        if not invoice_id:
            logger.error(f"CryptoBot: не удалось получить invoice_id из ответа: {invoice}")
            return None
            
        return (str(pay_url), int(invoice_id))
    except Exception as e:
        logger.error(f"CryptoBot: ошибка при создании счёта: {e}", exc_info=True)
        return None


async def get_usdt_rub_rate() -> Optional[Decimal]:
    try:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=rub"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    logger.warning(f"USDT/RUB: HTTP {resp.status}")
                    return None
                data = await resp.json()
                val = data.get("tether", {}).get("rub")
                if val is None:
                    return None
                return Decimal(str(val))
    except Exception as e:
        logger.warning(f"USDT/RUB: ошибка получения курса: {e}")
        return None


async def get_ton_usdt_rate() -> Optional[Decimal]:
    try:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=toncoin&vs_currencies=usd"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    logger.warning(f"TON/USD: HTTP {resp.status}")
                    return None
                data = await resp.json()
                usd = data.get("toncoin", {}).get("usd")
                if usd is None:
                    return None
                return Decimal(str(usd))
    except Exception as e:
        logger.warning(f"TON/USD: ошибка получения курса: {e}")
        return None


async def _start_ton_connect_process(user_id: int, transaction_payload: Dict) -> str:
    try:
        messages = transaction_payload.get("messages") or []
        if not messages:
            raise ValueError("transaction_payload.messages is empty")
        msg = messages[0]
        address = msg.get("address")
        amount = msg.get("amount")
        payload_text = msg.get("payload") or ""
        if not address or not amount:
            raise ValueError("address/amount are required in transaction message")
        params = {"amount": amount}
        if payload_text:
            params["text"] = str(payload_text)
        query = urlencode(params)
        return f"ton://transfer/{address}?{query}"
    except Exception as e:
        logger.error(f"TON генерация deep link не удалась: {e}")
        return "ton://transfer"


def _build_yoomoney_quickpay_url(
    wallet: str,
    amount: float,
    label: str,
    success_url: Optional[str] = None,
    targets: Optional[str] = None,
) -> str:
    try:
        params = {
            "receiver": wallet,
            "quickpay-form": "shop",
            "sum": f"{float(amount):.2f}",
            "label": label,
        }
        if success_url:
            params["successURL"] = success_url
        if targets:
            params["targets"] = targets
        base = "https://yoomoney.ru/quickpay/confirm.xml"
        return f"{base}?{urlencode(params)}"
    except Exception:
        return "https://yoomoney.ru/"


async def _yoomoney_find_payment(label: str) -> Optional[dict]:
    token = (get_setting("yoomoney_api_token") or "").strip()
    if not token:
        logger.warning("YooMoney: API токен не задан в настройках.")
        return None
    url = "https://yoomoney.ru/api/operation-history"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "label": label,
        "records": "5",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, headers=headers, timeout=15) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logger.warning(f"YooMoney: operation-history HTTP {resp.status}: {text}")
                    return None
                try:
                    payload = await resp.json()
                except Exception:
                    try:
                        payload = json.loads(text)
                    except Exception:
                        logger.warning("YooMoney: не удалось распарсить JSON operation-history")
                        return None
                ops = payload.get("operations") or []
                for op in ops:
                    if str(op.get("label")) == str(label) and str(op.get("direction")) == "in":
                        status = str(op.get("status") or "").lower()
                        if status == "success":
                            try:
                                amount = float(op.get("amount"))
                            except Exception:
                                amount = None
                            return {
                                "operation_id": op.get("operation_id"),
                                "amount": amount,
                                "datetime": op.get("datetime"),
                            }
                return None
    except Exception as e:
        logger.error(f"YooMoney: ошибка запроса operation-history: {e}", exc_info=True)
        return None


async def notify_admin_of_purchase(bot: Bot, metadata: dict):
    try:
        admin_id_raw = get_setting("admin_telegram_id")
        if not admin_id_raw:
            return
        admin_id = int(admin_id_raw)
        user_id = metadata.get('user_id')
        host_name = metadata.get('host_name')
        months = metadata.get('months')
        price = metadata.get('price')
        action = metadata.get('action')
        payment_method = metadata.get('payment_method') or 'Unknown'
        payment_method_map = {
            'Balance': 'Баланс',
            'Card': 'Карта',
            'Crypto': 'Крипто',
            'USDT': 'USDT',
            'TON': 'TON',
        }
        payment_method_display = payment_method_map.get(payment_method, payment_method)
        plan_id = metadata.get('plan_id')
        plan = get_plan_by_id(plan_id)
        plan_name = plan.get('plan_name', 'Unknown') if plan else 'Unknown'

        text = (
            "📥 Новая оплата\n"
            f"👤 Пользователь: {user_id}\n"
            f"🗺️ Хост: {host_name}\n"
            f"📦 Тариф: {plan_name} ({months} мес.)\n"
            f"💳 Метод: {payment_method_display}\n"
            f"💰 Сумма: {float(price):.2f} RUB\n"
            f"⚙️ Действие: {'Новый ключ' if action == 'new' else 'Продление'}"
        )
        await bot.send_message(admin_id, text)
    except Exception as e:
        logger.warning(f"notify_admin_of_purchase не удался: {e}")

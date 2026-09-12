"use strict";

const telegram = window.Telegram && window.Telegram.WebApp;
const statusText = document.getElementById("status");
const phoneForm = document.getElementById("phone-form");
const codeForm = document.getElementById("code-form");
const passwordForm = document.getElementById("password-form");
const success = document.getElementById("success");
const cancelButton = document.getElementById("cancel");
const closeButton = document.getElementById("close");
const togglePasswordButton = document.getElementById("toggle-password");
const progressItems = Array.from(document.querySelectorAll("[data-progress]"));

let browserToken = "";
let csrfToken = "";
let initData = "";

function applyTheme() {
  const theme = telegram && telegram.colorScheme === "dark" ? "dark" : "light";
  document.documentElement.dataset.theme = theme;
  if (telegram) {
    telegram.setHeaderColor("bg_color");
    telegram.setBackgroundColor("bg_color");
  }
}

function setStatus(message, kind = "info") {
  statusText.textContent = message;
  statusText.dataset.kind = kind;
}

function setLoading(button, loading) {
  button.disabled = loading;
  button.textContent = loading
    ? button.dataset.loadingLabel
    : button.dataset.idleLabel;
}

function updateProgress(step) {
  const order = ["phone", "code", "password"];
  const activeIndex = order.indexOf(step);
  progressItems.forEach((item) => {
    const itemIndex = order.indexOf(item.dataset.progress);
    item.classList.toggle("active", itemIndex === activeIndex);
    item.classList.toggle("complete", activeIndex === -1 || itemIndex < activeIndex);
  });
}

function showStep(form, step) {
  phoneForm.hidden = form !== phoneForm;
  codeForm.hidden = form !== codeForm;
  passwordForm.hidden = form !== passwordForm;
  success.hidden = true;
  cancelButton.hidden = false;
  updateProgress(step);
}

async function api(path, body) {
  const headers = {
    "Content-Type": "application/json",
    "X-Telegram-Init-Data": initData,
  };
  if (browserToken) {
    headers["X-Auth-Session"] = browserToken;
    headers["X-CSRF-Token"] = csrfToken;
  }

  let response;
  try {
    response = await fetch(path, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
      credentials: "omit",
      cache: "no-store",
    });
  } catch (_) {
    throw new Error(
      "Не удалось связаться с Telegram. Проверьте соединение и попробуйте снова."
    );
  }

  if (!response.ok) {
    let detail = "Что-то пошло не так. Попробуйте ещё раз.";
    try {
      const error = await response.json();
      if (typeof error.detail === "string") {
        detail = error.detail;
      }
    } catch (_) {
      // Keep the generic, non-sensitive message.
    }
    throw new Error(detail);
  }
  return response.status === 204 ? null : response.json();
}

async function bootstrap() {
  applyTheme();
  if (!telegram || !telegram.initData) {
    setStatus("Откройте форму кнопкой «Добавить аккаунт» в боте.", "error");
    return;
  }
  telegram.ready();
  telegram.expand();
  telegram.onEvent("themeChanged", applyTheme);
  initData = telegram.initData;

  const fragment = new URLSearchParams(window.location.hash.slice(1));
  let launchToken = fragment.get("token") || "";
  window.history.replaceState(null, "", window.location.pathname);
  if (!launchToken) {
    setStatus("Ссылка больше не действует. Вернитесь в бот и начните заново.", "error");
    return;
  }

  try {
    const result = await api("/api/auth/bootstrap", { launch_token: launchToken });
    launchToken = "";
    browserToken = result.sessionToken;
    csrfToken = result.csrfToken;
    setStatus("Введите номер Telegram-аккаунта.");
    showStep(phoneForm, "phone");
    document.getElementById("phone").focus();
  } catch (error) {
    launchToken = "";
    setStatus(error.message, "error");
  }
}

phoneForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const input = document.getElementById("phone");
  let phone = input.value.trim();
  input.value = "";
  const button = phoneForm.querySelector("button[type='submit']");
  setLoading(button, true);
  setStatus("Отправляем код…");
  try {
    await api("/api/auth/phone", { phone });
    phone = "";
    setStatus("Код отправлен в Telegram.", "success");
    showStep(codeForm, "code");
    document.getElementById("code").focus();
  } catch (error) {
    phone = "";
    setStatus(error.message, "error");
  } finally {
    setLoading(button, false);
  }
});

codeForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const input = document.getElementById("code");
  let code = input.value.trim();
  input.value = "";
  const button = codeForm.querySelector("button[type='submit']");
  setLoading(button, true);
  setStatus("Проверяем код…");
  try {
    const result = await api("/api/auth/code", { code });
    code = "";
    if (result.next === "password") {
      setStatus("На аккаунте включена 2FA. Введите пароль.");
      showStep(passwordForm, "password");
      document.getElementById("password").focus();
      return;
    }
    showSuccess();
  } catch (error) {
    code = "";
    setStatus(error.message, "error");
  } finally {
    setLoading(button, false);
  }
});

passwordForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const input = document.getElementById("password");
  let password = input.value;
  input.value = "";
  const button = passwordForm.querySelector("button[type='submit']");
  setLoading(button, true);
  setStatus("Подключаем аккаунт…");
  try {
    await api("/api/auth/password", { password });
    password = "";
    showSuccess();
  } catch (error) {
    password = "";
    setStatus(error.message, "error");
  } finally {
    setLoading(button, false);
  }
});

togglePasswordButton.addEventListener("click", () => {
  const input = document.getElementById("password");
  const showing = input.type === "text";
  input.type = showing ? "password" : "text";
  togglePasswordButton.textContent = showing ? "Показать" : "Скрыть";
  togglePasswordButton.setAttribute("aria-pressed", String(!showing));
  togglePasswordButton.setAttribute(
    "aria-label",
    showing ? "Показать пароль" : "Скрыть пароль"
  );
  input.focus();
});

function showSuccess() {
  phoneForm.hidden = true;
  codeForm.hidden = true;
  passwordForm.hidden = true;
  cancelButton.hidden = true;
  success.hidden = false;
  updateProgress("complete");
  setStatus("Аккаунт подключён.", "success");
  browserToken = "";
  csrfToken = "";
  initData = "";
}

cancelButton.addEventListener("click", async () => {
  cancelButton.disabled = true;
  cancelButton.textContent = "Отменяем…";
  try {
    await api("/api/auth/cancel", {});
  } catch (_) {
    // The server timeout remains the cleanup fallback.
  } finally {
    browserToken = "";
    csrfToken = "";
    initData = "";
    telegram.close();
  }
});

closeButton.addEventListener("click", () => {
  if (telegram) {
    telegram.close();
  }
});

bootstrap();

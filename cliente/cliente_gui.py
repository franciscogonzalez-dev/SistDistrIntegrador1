import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import time
import Pyro5.api
import Pyro5.errors

from comun.reloj_lamport import RelojLamport
from comun import config
from cliente.cliente import encontrar_primario, OPCIONES_OFERTA

@Pyro5.api.expose
class GuiCallback:
    """Recibe eventos push del servidor y los manda a la GUI con root.after"""
    def __init__(self, app):
        self.app = app

    def notificar_estado(self, estado: dict):
        # Despachar al hilo principal de Tkinter de forma segura
        self.app.root.after(0, self.app.actualizar_estado_remoto, estado)


class ClienteSubastaGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Subasta Distribuida - Cliente")
        self.root.geometry("520x620")

        self.reloj = RelojLamport()
        self.seq_visto = -1
        self.primario = None
        self.cliente_id = None
        self.uri_callback = None
        self.iniciada = False
        self.cerrada = False
        self.ronda_finalizada = False
        self.tiempo_restante_servidor = 0.0
        self.tiempo_actualizacion = 0.0

        self._crear_widgets()
        self._tick_reloj_local()

    def _tick_reloj_local(self):
        """Actualiza la cuenta regresiva en tiempo real en la UI"""
        if self.iniciada and not self.cerrada:
            transcurrido = time.time() - self.tiempo_actualizacion
            restante = max(0.0, self.tiempo_restante_servidor - transcurrido)
            self.lbl_tiempo.config(text=f"Tiempo restante: {restante:.1f} s")
        elif not self.iniciada:
            if self.primario:
                self.lbl_tiempo.config(text="Esperando inicio del servidor...")
            else:
                self.lbl_tiempo.config(text="Tiempo restante: - s")
        else:
            self.lbl_tiempo.config(text="Tiempo restante: 0.0 s (Cerrada)")

        self.root.after(200, self._tick_reloj_local)

    def _crear_widgets(self):
        # --- Panel de Conexión ---
        frame_conn = ttk.LabelFrame(self.root, text="Conexión", padding=10)
        frame_conn.pack(fill="x", padx=10, pady=5)

        ttk.Label(frame_conn, text="ID Cliente:").pack(side="left", padx=5)
        self.ent_id = ttk.Entry(frame_conn, width=15)
        self.ent_id.insert(0, "ana")
        self.ent_id.pack(side="left", padx=5)

        self.btn_conectar = ttk.Button(frame_conn, text="Conectar", command=self.conectar)
        self.btn_conectar.pack(side="left", padx=5)

        self.lbl_nodo = ttk.Label(frame_conn, text="Desconectado", foreground="gray")
        self.lbl_nodo.pack(side="right", padx=5)

        # --- Panel de Estado ---
        frame_estado = ttk.LabelFrame(self.root, text="Estado de la Subasta", padding=10)
        frame_estado.pack(fill="x", padx=10, pady=5)

        self.lbl_articulo = ttk.Label(frame_estado, text="Artículo: -", font=("Segoe UI", 11, "bold"), justify="left")
        self.lbl_articulo.pack(anchor="w", pady=2)

        self.lbl_transicion = ttk.Label(frame_estado, text="", font=("Segoe UI", 11, "italic"), foreground="orange")
        self.lbl_transicion.pack(anchor="w", pady=2)

        self.lbl_oferta = ttk.Label(frame_estado, text="Mejor Oferta: $0.00", font=("Segoe UI", 13, "bold"), foreground="green")
        self.lbl_oferta.pack(anchor="w", pady=2)

        self.lbl_postor = ttk.Label(frame_estado, text="Líder: Ninguno")
        self.lbl_postor.pack(anchor="w", pady=2)

        self.lbl_tiempo = ttk.Label(frame_estado, text="Tiempo restante: - s")
        self.lbl_tiempo.pack(anchor="w", pady=2)

        self.lbl_lamport = ttk.Label(frame_estado, text="Lamport: 0 | Seq: 0", font=("Segoe UI", 8), foreground="gray")
        self.lbl_lamport.pack(anchor="w", pady=2)

        # --- Panel de Ofertas ---
        frame_ofertas = ttk.LabelFrame(self.root, text="Hacer Oferta", padding=10)
        frame_ofertas.pack(fill="x", padx=10, pady=5)

        self.btn_ofertas = []
        for key, monto in OPCIONES_OFERTA.items():
            btn = ttk.Button(
                frame_ofertas,
                text=f"+${int(monto)}",
                state="disabled",
                command=lambda m=monto: self.hacer_oferta_async(m)
            )
            btn.pack(side="left", expand=True, fill="x", padx=5)
            self.btn_ofertas.append(btn)

        # --- Historial de Eventos ---
        frame_log = ttk.LabelFrame(self.root, text="Historial de Eventos", padding=10)
        frame_log.pack(fill="both", expand=True, padx=10, pady=5)

        self.txt_log = scrolledtext.ScrolledText(frame_log, height=10, state="disabled")
        self.txt_log.pack(fill="both", expand=True)

    def log(self, mensaje: str):
        self.txt_log.config(state="normal")
        self.txt_log.insert("end", f"{mensaje}\n")
        self.txt_log.see("end")
        self.txt_log.config(state="disabled")

    def conectar(self):
        self.cliente_id = self.ent_id.get().strip()
        if not self.cliente_id:
            messagebox.showwarning("Atención", "Ingresá un ID de cliente válido.")
            return

        self.btn_conectar.config(state="disabled")
        self.ent_id.config(state="disabled")
        self.log(f"Buscando primario para '{self.cliente_id}'...")

        threading.Thread(target=self._tarea_conectar, daemon=True).start()

    def _tarea_conectar(self):
        try:
            self.primario = encontrar_primario()
            self.primario._pyroClaimOwnership()
            self.primario._pyroTimeout = 6.0

            # Daemon local para callbacks
            daemon_cliente = Pyro5.api.Daemon()
            callback = GuiCallback(self)
            self.uri_callback = daemon_cliente.register(callback)
            threading.Thread(target=daemon_cliente.requestLoop, daemon=True).start()

            self.primario.subscribir_cliente(str(self.uri_callback))
            estado_ini = self.primario.obtener_estado()

            self.root.after(0, self._al_conectar_exito, estado_ini)
        except Exception as e:
            self.root.after(0, self._al_conectar_fallo, str(e))

    def _al_conectar_exito(self, estado: dict):
        self.lbl_nodo.config(text="Conectado al cluster", foreground="green")
        self.log("Conectado con éxito al primario.")
        self.actualizar_estado_remoto(estado)

    def _al_conectar_fallo(self, err: str):
        self.log(f"Error al conectar: {err}")
        self.btn_conectar.config(state="normal")
        self.ent_id.config(state="normal")
        messagebox.showerror("Error", f"No se pudo conectar al cluster:\n{err}")

    def actualizar_estado_remoto(self, estado: dict):
        self.reloj.actualizar(estado.get("clock_lamport", 0))
        
        nueva_iniciada = estado.get("iniciada", False)
        nueva_cerrada = estado.get("cerrada", False)
        nuevo_seq = estado.get("seq_op", 0)
        
        self.tiempo_restante_servidor = estado.get("tiempo_restante_seg", 0.0)
        self.tiempo_actualizacion = time.time()

        # Detectar transiciones de estado para loguear apropiadamente
        estaba_iniciada = self.iniciada
        estaba_cerrada = self.cerrada
        seq_anterior = self.seq_visto

        self.iniciada = nueva_iniciada
        self.cerrada = nueva_cerrada
        self.seq_visto = nuevo_seq

        auto = estado.get('articulo', {})
        if isinstance(auto, dict):
            marca = auto.get("marca", "-")
            modelo = auto.get("modelo", "-")
            anio = auto.get("anio", "-")
            km = auto.get("kilometraje", "-")
            fallas = auto.get("fallas_defectos", "-")
            imgs = auto.get("imagenes", [])
            imgs_str = ", ".join(imgs) if imgs else "Ninguna"
            texto_articulo = f"Auto: {marca} {modelo} ({anio})\nKM: {km} | Fallas: {fallas}\nImágenes: {imgs_str}"
            nombre_corto = f"{marca} {modelo}"
        else:
            texto_articulo = f"Artículo: {auto}"
            nombre_corto = str(auto)
            
        self.lbl_articulo.config(text=texto_articulo)
        
        # Mensaje de transicion si lo hay
        msg_trans = estado.get("mensaje_transicion", "")
        self.lbl_transicion.config(text=msg_trans)

        self.lbl_oferta.config(text=f"Mejor Oferta: ${estado.get('mejor_oferta', 0.0):.2f}")
        self.lbl_postor.config(text=f"Líder: {estado.get('mejor_postor') or 'Ninguno'}")
        self.lbl_lamport.config(text=f"Lamport: {self.reloj.valor()} | Seq: {self.seq_visto}")

        subasta_activa = self.iniciada and not self.cerrada
        for btn in self.btn_ofertas:
            btn.config(state="normal" if subasta_activa else "disabled")

        if estado.get("ronda_finalizada"):
            if not self.ronda_finalizada:
                self.log("🏆 [RONDA FINALIZADA] No quedan más autos para subastar. ¡Gracias por participar!")
                for btn in self.btn_ofertas:
                    btn.config(state="disabled")
            self.ronda_finalizada = True
        elif self.cerrada:
            if not estaba_cerrada:
                ganador = estado.get("mejor_postor")
                if ganador:
                    self.log(f"🏁 [FINALIZADA] Ganador: {ganador} por ${estado.get('mejor_oferta', 0.0):.2f}")
                else:
                    self.log("🏁 [FINALIZADA] La subasta finalizó sin ofertas.")
                if msg_trans:
                    self.log(f"⏳ {msg_trans}")
        elif self.iniciada:
            if not estaba_iniciada:
                self.log(f"🚀 [INICIO] ¡Subasta iniciada para '{nombre_corto}'! Ventana de 30s activa.")
            elif nuevo_seq > seq_anterior and estado.get("mejor_postor"):
                self.log(f"📢 [NUEVA MEJOR OFERTA] ${estado.get('mejor_oferta', 0.0):.2f} ({estado.get('mejor_postor')})")

    def hacer_oferta_async(self, incremento: float):
        # Ejecutar en hilo secundario para no congelar la UI si hay failover
        threading.Thread(target=self._tarea_ofertar, args=(incremento,), daemon=True).start()

    def _tarea_ofertar(self, incremento: float):
        clock_envio = self.reloj.tick()
        try:
            if self.primario:
                self.primario._pyroClaimOwnership()
            resp = self.primario.ofertar(self.cliente_id, incremento, clock_envio, self.seq_visto)
        except (Pyro5.errors.CommunicationError, Pyro5.errors.PyroError) as err:
            self.root.after(0, self.log, "⚠️ Problema con primario. Reconectando...")
            try:
                self.primario = encontrar_primario()
                self.primario._pyroClaimOwnership()
                self.primario._pyroTimeout = 6.0
                self.primario.subscribir_cliente(str(self.uri_callback))
                resp = self.primario.ofertar(self.cliente_id, incremento, clock_envio, self.seq_visto)
            except Exception as e:
                self.root.after(0, self.log, f"❌ Error en reconexión: {e}")
                return

        self.root.after(0, self._procesar_resultado_oferta, resp)

    def _procesar_resultado_oferta(self, resp: dict):
        self.log(f"👉 {resp.get('motivo', '')}")
        if "estado" in resp:
            self.actualizar_estado_remoto(resp["estado"])


if __name__ == "__main__":
    root = tk.Tk()
    app = ClienteSubastaGUI(root)
    root.mainloop()
